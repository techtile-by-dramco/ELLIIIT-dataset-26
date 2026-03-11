# Code to perform acoustic measurements using an omni-directional speaker
#
# -------------------------------------------------------------------------
# Configuration parameters loaded from JSON file
#
# room_dim             : Physical room dimensions (meters)
# speaker_coordinates  : 3D coordinates of the sound source (x, y, z)
# chirp_f_start        : Start frequency of the excitation chirp (Hz)
# chirp_f_stop         : Stop frequency of the excitation chirp (Hz)
# chirp_duration       : Total duration of the chirp (s)
# chirp_DC             : Chirp DC offset voltage
# chirp_ampl           : Output amplitude of the chirp signal (V)
# max_val_out          : Maximum allowed output voltage on NI-DAQ AO channel
# min_val_out          : Minimum allowed output voltage on NI-DAQ AO channel
# max_val_in           : Maximum expected input voltage on NI-DAQ AI channel
# min_val_in           : Minimum expected input voltage on NI-DAQ AI channel
# sample_rate          : Sampling frequency for NI-DAQ acquisition tasks (Hz)
# plot_signals         : Boolean — enable/disable plotting of recorded signals
# get_system_info      : Boolean — print NI-DAQ system information on startup
# -------------------------------------------------------------------------

import math
import json
import csv
import subprocess
from pathlib import Path
import time
from datetime import datetime
import pyroomacoustics as pra
import numpy as np
from scipy.signal import chirp, convolve
import nidaqmx as ni
from nidaqmx.constants import AcquisitionType, TaskMode

from dicts import mic_dict, source_dict

BASE_DIR    = Path(__file__).parent.resolve()
CONFIG_PATH = BASE_DIR / "config.json"
EXE_SYNC    = BASE_DIR / "sync_exe_files" / "sync.exe"
EXE_CLEANUP = BASE_DIR / "sync_exe_files" / "reset_sync.exe"
SAVE_DIR     = BASE_DIR / "results"

def load_config(path: Path = CONFIG_PATH) -> dict:
    if not path.exists():
        raise FileNotFoundError(f"config not found: {path}")
    with open(path, "r") as f:
        config = json.load(f)
    _validate_config(config)
    return config

def save_config(config: dict, path: Path = CONFIG_PATH) -> None:
    with open(path, "w") as f:
        json.dump(config, f, indent=2)
    print(f"config saved: {path}")

def update_config(key: str, value, path: Path = CONFIG_PATH) -> None:
    config = load_config(path)
    if key not in config:
        raise KeyError(f"Key '{key}' not found in config")
    config[key] = value
    save_config(config, path)


def _validate_config(config: dict) -> None:
    required = [
        "sample_rate", "chirp_f_start", "chirp_f_stop",
        "chirp_duration", "chirp_ampl", "chirp_DC",
        "speaker_coordinates", "get_system_info",
    ]
    missing = [k for k in required if k not in config]
    if missing:
        raise ValueError(f"Config is missing keys: {missing}")
    if config["chirp_f_start"] >= config["chirp_f_stop"]:
        raise ValueError("chirp_f_start >= chirp_f_stop")
    if config["chirp_duration"] <= 0:
        raise ValueError("chirp_duration < 0")


def read_system() -> None:
    system = ni.system.System.local()
    for device in system.devices:
        print('CO-channels')
        for channel in device.co_physical_chans:
            print(channel)
        print('AI-channels')
        for channel in device.ai_physical_chans:
            print(channel)
        print('AO-channels')
        for channel in device.ao_physical_chans:
            print(channel)
        print('Terminals')
        for tr in device.terminals:
            print(tr)


def DAQ(config: dict, RIR_excitation: np.ndarray):
    n_out = np.size(RIR_excitation)
    n_in  = 2 * n_out
    used_channels = []
    unused_channels = []

    with ni.Task(new_task_name='out_slot2') as out1, ni.Task(new_task_name="in") as in1:

        # Channel registration
        for mic_id, (channel, coords) in mic_dict.items():
            in1.ai_channels.add_ai_voltage_chan(channel)
            used_channels.append([mic_id, channel, coords])
            #print(f"[mic]    {mic_id}  {channel}  coords={coords}")

        for source_id, (channel, coords) in source_dict.items():
            out1.ao_channels.add_ao_voltage_chan(channel)
            #print(f"[source] {source_id}  {channel}  coords={coords}")

        # Clock & trigger setup
        # Reference Clock Synchronization setup (get backplane external PXIe_Clk100,
        # optimized and shared from the Synchronization module)
        # Setup same reference clock and triggers for synchronization over PXI_Trig
        out1.timing.ref_clk_src = "PXIe_Clk100"
        out1.timing.ref_clk_rate = 100000000
        out1.timing.cfg_samp_clk_timing(rate=config["sample_rate"], samps_per_chan=n_out) # sample_mode=AcquisitionType.CONTINUOUS
        out1.triggers.sync_type.SLAVE = True

        in1.timing.ref_clk_src = "PXIe_Clk100"
        in1.timing.ref_clk_rate = 100000000
        in1.timing.cfg_samp_clk_timing(rate=config["sample_rate"], samps_per_chan=n_in, sample_mode=AcquisitionType.FINITE)
        in1.triggers.sync_type.MASTER = True
        
        out1.control(TaskMode.TASK_COMMIT)
        in1.control(TaskMode.TASK_COMMIT)

        out1.triggers.start_trigger.cfg_dig_edge_start_trig(in1.triggers.start_trigger.term)
        out1.write(RIR_excitation, auto_start=False)

        out1.start()
        in1.start()

        RX_raw = in1.read(number_of_samples_per_channel=n_in)

        out1.stop()
        in1.stop()

    RX = np.asarray(RX_raw)

    if RX.ndim == 1:
        RX = RX[np.newaxis, :]

    RX = RX[:, 1:]

    RX = np.ascontiguousarray(RX, dtype=float)

    valid_used_channels = []
    valid_rx_indices = []

    for idx, channel_info in enumerate(used_channels):
        rir_mean = np.mean(RX[idx])

        if rir_mean < 1.5:
            unused_channels.append(channel_info)
            print(f"Channel {channel_info[0]} marked as HARDWARE ERROR (mean={rir_mean:.2f})")
        else:
            valid_used_channels.append(channel_info)
            valid_rx_indices.append(idx)

    used_channels = valid_used_channels
    RX = RX[valid_rx_indices, :]

    return RX, used_channels, unused_channels


def excitateChirp(config: dict):
    fs = config["sample_rate"]
    duration = config["chirp_duration"]
    n_meas = int(fs * duration)
    t = np.linspace(0, duration, n_meas, endpoint=False)

    chirpExcitation = config["chirp_ampl"] * chirp(t, f0=config["chirp_f_start"], f1=config["chirp_f_stop"], t1=duration, method="log")

    RX_data, used_channels, unused_channels = DAQ(config, chirpExcitation)
    return chirpExcitation, used_channels, unused_channels, RX_data


def calculateRIRFFT(rx_channel: np.ndarray, chirpExcitation: np.ndarray) -> np.ndarray:
    N = len(chirpExcitation)
    nfft = 2 * N
    X = np.fft.rfft(chirpExcitation, n=nfft)
    Y = np.fft.rfft(rx_channel, n=nfft)

    eps = 1e-6 * np.max(np.abs(X))
    H = Y * np.conj(X) / (np.abs(X) ** 2 + eps ** 2)

    rir = np.fft.irfft(H)
    return rir[:N].real

def calculateRIRDeconvolution(config: dict, RX_data: np.ndarray, chirpExcitation: np.ndarray) -> list:
    fs = config["sample_rate"]
    duration = config["chirp_duration"]
    f1 = config["chirp_f_start"]
    f2 = config["chirp_f_stop"]
    amp = config["chirp_ampl"]
    N = len(chirpExcitation)

    L = duration / np.log(f2 / f1)
    p = np.arange(N) / fs
    weight   = (f1 / f2) * np.exp(+p / L)
    inv_filter = amp * np.flipud(chirpExcitation) * weight

    measured_RIRs = []
    for rx in RX_data:
        conv = convolve(rx, inv_filter, mode="full")
        rir  = conv[N-1 : 2*N - 1]
        measured_RIRs.append(rir)

    return measured_RIRs

def saveReceivedESS(RX_data: np.ndarray) -> list[np.ndarray]:
    return [np.array(rx) for rx in RX_data]

def calculateRIRS(config: dict, RX_data: np.ndarray, chirpExcitation: np.ndarray, method: str = "save_ess", ) -> list:
    if method == "deconv":
        return calculateRIRDeconvolution(config, RX_data, chirpExcitation)
    elif method == "fft":
        return [calculateRIRFFT(rx, chirpExcitation) for rx in RX_data]
    elif method == "save_ess":
        return saveReceivedESS(RX_data)
                
def save_RIRs_to_csv(config: dict, used_channels: list, unused_channels: list, chirpExcitation: np.ndarray, measuredRIRs: list, filename: Path) -> None:
    with open(filename, mode="w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["speaker", "duration", "f_start", "f_stop", "chirp_amp", "microphone_coordinates", "microphone_label", "values"])
        for idx, (mic_id, channel, coords) in enumerate(used_channels):
            writer.writerow([
                config["speaker_coordinates"],
                config["chirp_duration"],
                config["chirp_f_start"],
                config["chirp_f_stop"],
                config["chirp_ampl"],
                tuple(coords),
                mic_id,
                measuredRIRs[idx].tolist(),
            ])
        for idx, (mic_id, channel, coords) in enumerate(unused_channels):
            writer.writerow([
                config["speaker_coordinates"],
                config["chirp_duration"],
                config["chirp_f_start"],
                config["chirp_f_stop"],
                config["chirp_ampl"],
                tuple(coords),
                mic_id,
                "unused",
            ])
        writer.writerow([
            config["speaker_coordinates"],
            config["chirp_duration"],
            config["chirp_f_start"],
            config["chirp_f_stop"],
            config["chirp_ampl"],
            "chirp_excitation",
            "chirp_excitation",
            chirpExcitation.tolist(),
        ])

    print(f"RIRs saved: {filename}")


def _run_exe(exe_path: Path, label: str) -> None:
    if not exe_path.exists():
        raise FileNotFoundError(f"{label} executable not found: {exe_path}")
    result = subprocess.run(str(exe_path), shell=True, stdout=subprocess.PIPE, text=True)
    if result.returncode != 0:
        raise RuntimeError(
            f"{label} exited with code {result.returncode}:\n{result.stderr}"
        )
    if result.stdout:
        print(f"[{label}] {result.stdout.strip()}")


def run_acoustic_measurement(base_config: dict, overrides: dict) -> dict:
    config = load_config()      # reload from disk
    config.update(overrides)    # apply server-sent overrides before anything else

    
    if config.get("get_system_info"):
        read_system()

    _run_exe(EXE_SYNC, "sync")
    t_start = time.time()
    
    try:
        chirpExcitation, used_channels, unused_channels, RX_data = excitateChirp(config)
    except Exception as exc:
        _run_exe(EXE_CLEANUP, "cleanup")
        raise RuntimeError(f"DAQ acquisition failed: {exc}") from exc

    _run_exe(EXE_CLEANUP, "cleanup")

    measuredSignal = calculateRIRS(config, RX_data, chirpExcitation, method="save_ess")

    sx, sy, sz = config["speaker_coordinates"]
    timestamp  = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename   = SAVE_DIR / f"Measured_Signal_{sx}_{sy}_{sz}_{timestamp}.csv"
    save_RIRs_to_csv(config, used_channels, unused_channels, chirpExcitation, measuredSignal, filename)

    return {
        "csv_file":   str(filename),
        "n_mics":     len(used_channels),
        "duration_s": round(time.time() - t_start, 4),
    }