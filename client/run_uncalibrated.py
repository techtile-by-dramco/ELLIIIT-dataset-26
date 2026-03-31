import argparse
import json
import logging
import os
import queue
import socket
import sys
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import tools
import uhd
import yaml
import zmq
import runtime_storage

# =============================================================================
#                           Experiment Configuration
# =============================================================================
CMD_DELAY = 0.05
CLOCK_TIMEOUT = 1000
INIT_DELAY = 0.2
RATE = 250e3
CAPTURE_TIME = 10
FREQ = 0
meas_id = 0
exp_id = 0
PEAK_AMPLITUDE_HIGH_THRESHOLD = 1.0
PEAK_AMPLITUDE_LOW_THRESHOLD = 0.01
# =============================================================================

context = zmq.Context()
HOSTNAME_RAW = socket.gethostname()
HOSTNAME = HOSTNAME_RAW[4:] if len(HOSTNAME_RAW) > 4 else HOSTNAME_RAW
file_open = False
RUNTIME_OUTPUT_DIR = None
data_file = None
data_file_path = None
log_file_handler = None
error_log_lock = threading.Lock()


class LogFormatter(logging.Formatter):
    """Custom log formatter that prints timestamps with fractional seconds."""

    @staticmethod
    def pp_now():
        now = datetime.now()
        return "{:%H:%M}:{:05.2f}".format(now, now.second + now.microsecond / 1e6)

    def formatTime(self, record, datefmt=None):
        converter = self.converter(record.created)
        if datefmt:
            formatted_date = converter.strftime(datefmt)
        else:
            formatted_date = LogFormatter.pp_now()
        return formatted_date


class ColoredFormatter(LogFormatter):
    """Console formatter with ANSI colors per level."""

    COLORS = {
        logging.DEBUG: "\033[36m",
        logging.INFO: "\033[32m",
        logging.WARNING: "\033[33m",
        logging.ERROR: "\033[31m",
        logging.CRITICAL: "\033[35m",
    }
    RESET = "\033[0m"

    def format(self, record):
        color = self.COLORS.get(record.levelno, "")
        reset = self.RESET if color else ""
        record.levelname = f"{color}{record.levelname}{reset}"
        return super().format(record)


def fmt(val):
    try:
        return f"{float(val):.3f}"
    except Exception:
        return str(val)


global logger
global begin_time

begin_time = 2.0

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

console = logging.StreamHandler()
logger.addHandler(console)

formatter = LogFormatter(
    fmt="[%(asctime)s] [%(levelname)s] (%(threadName)-10s) %(message)s"
)
console.setFormatter(ColoredFormatter(fmt=formatter._fmt))

def setup_clock(usrp, clock_src, num_mboards):
    usrp.set_clock_source(clock_src)
    logger.debug("Now confirming lock on clock signals...")
    end_time = datetime.now() + timedelta(milliseconds=CLOCK_TIMEOUT)
    for i in range(num_mboards):
        is_locked = usrp.get_mboard_sensor("ref_locked", i)
        while (not is_locked) and (datetime.now() < end_time):
            time.sleep(1e-3)
            is_locked = usrp.get_mboard_sensor("ref_locked", i)
        if not is_locked:
            logger.error("Unable to confirm clock signal locked on board %d", i)
            append_error_log(
                "CLOCK_LOCK_FAILED",
                f"Unable to confirm clock signal locked on board {i}.",
                board_index=i,
            )
            return False
        logger.debug("Clock signals are locked")
    return True


def setup_pps(usrp, pps):
    logger.debug("Setting PPS")
    usrp.set_time_source(pps)
    return True


def configure_file_logging(output_dir):
    global RUNTIME_OUTPUT_DIR, log_file_handler

    RUNTIME_OUTPUT_DIR = Path(output_dir)
    RUNTIME_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    if log_file_handler is not None:
        logger.removeHandler(log_file_handler)
        log_file_handler.close()

    log_file_handler = logging.FileHandler(
        RUNTIME_OUTPUT_DIR / f"{Path(__file__).stem}.log",
        mode="w",
    )
    log_file_handler.setFormatter(formatter)
    logger.addHandler(log_file_handler)


def build_output_path(file_name):
    if RUNTIME_OUTPUT_DIR is None:
        raise RuntimeError("Runtime output directory is not configured.")
    return RUNTIME_OUTPUT_DIR / file_name


def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()


def _json_safe(value):
    if isinstance(value, np.ndarray):
        return value.tolist()
    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, Path):
        return str(value)
    return value


def append_error_log(error_type, message, **fields):
    if RUNTIME_OUTPUT_DIR is None:
        logger.error("Unable to write error log before runtime output is configured.")
        return

    entry = {
        "timestamp_utc": utc_now_iso(),
        "hostname": HOSTNAME,
        "error_type": error_type,
        "message": str(message),
    }
    current_context = {
        "experiment_id": globals().get("exp_id"),
        "cycle_id": globals().get("meas_id"),
        "file_name": globals().get("file_name"),
    }
    for key, value in current_context.items():
        if value not in (None, "", 0):
            entry[key] = _json_safe(value)
    entry.update({key: _json_safe(value) for key, value in fields.items()})

    error_log_path = build_output_path("error.log")
    try:
        with error_log_lock:
            with open(error_log_path, "a", encoding="utf-8") as handle:
                handle.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except Exception as ex:
        logger.error("Failed to append to error log %s: %s", error_log_path, ex)


def append_result_record(record):
    global data_file

    if data_file is None or data_file.closed:
        raise RuntimeError("Result file is not open.")

    safe_record = {key: _json_safe(value) for key, value in record.items()}
    data_file.write(json.dumps(safe_record, ensure_ascii=False) + "\n")
    data_file.flush()


def print_tune_result(tune_res):
    logger.debug(
        "Tune Result:\n    Target RF  Freq: %s MHz\n    Actual RF  Freq: %s MHz\n    Target DSP Freq: %s MHz\n    Actual DSP Freq: %s MHz",
        fmt(tune_res.target_rf_freq / 1e6),
        fmt(tune_res.actual_rf_freq / 1e6),
        fmt(tune_res.target_dsp_freq / 1e6),
        fmt(tune_res.actual_dsp_freq / 1e6),
    )


def tune_usrp_rx_only(usrp, freq, channels, at_time):
    """Synchronously tune RX channels only."""
    usrp.set_command_time(uhd.types.TimeSpec(at_time))
    rx_freq = freq - 1e3
    rreq = uhd.types.TuneRequest(rx_freq)
    rreq.rf_freq = rx_freq
    rreq.target_freq = rx_freq
    rreq.dsp_freq = 0.0
    rreq.rf_freq_policy = uhd.types.TuneRequestPolicy(ord("M"))
    rreq.dsp_freq_policy = uhd.types.TuneRequestPolicy(ord("M"))
    rreq.args = uhd.types.DeviceAddr("mode_n=fractional")
    for chan in channels:
        logger.debug("RX tuning channel %d...", chan)
        print_tune_result(usrp.set_rx_freq(rreq, chan))
    usrp.clear_command_time()
    while not usrp.get_rx_sensor("lo_locked").to_bool():
        print(".")
        time.sleep(0.01)
    logger.info("RX LO is locked")


def wait_till_go_from_server(ip, _connect=True):
    global meas_id, exp_id, file_open, data_file, data_file_path, file_name
    logger.debug("Connecting to server %s.", ip)
    sync_socket = context.socket(zmq.SUB)
    alive_socket = context.socket(zmq.REQ)
    alive_socket.setsockopt(zmq.LINGER, 0)

    sync_port = str(globals().get("SYNC_PORT", "5557"))
    alive_port = str(globals().get("ALIVE_PORT", "5558"))

    sync_socket.connect(f"tcp://{ip}:{sync_port}")
    alive_socket.connect(f"tcp://{ip}:{alive_port}")
    sync_socket.subscribe("")

    logger.debug("Sending ALIVE")
    alive_socket.send_string(HOSTNAME)
    alive_reply = alive_socket.recv_string()
    logger.debug("ALIVE acknowledged: %s", alive_reply)
    logger.debug("Waiting on SYNC from server %s.", ip)

    meas_id, unique_id = sync_socket.recv_string().split(" ")
    exp_id = unique_id
    file_name = f"data_{HOSTNAME}_{unique_id}_{meas_id}"

    next_data_file_path = build_output_path(f"data_{HOSTNAME}_{unique_id}.txt")
    if data_file is None or data_file_path != next_data_file_path:
        if data_file is not None and not data_file.closed:
            data_file.close()
        data_file = open(next_data_file_path, "a", encoding="utf-8")
        data_file_path = next_data_file_path
        file_open = True

    logger.debug(meas_id)

    alive_socket.close()
    sync_socket.close()


def send_usrp_done_mode(ip):
    done_port = str(globals().get("DONE_PORT", globals().get("DATA_PORT", "5559")))
    done_mode_socket = context.socket(zmq.REQ)
    done_mode_socket.setsockopt(zmq.LINGER, 0)
    done_mode_socket.connect(f"tcp://{ip}:{done_port}")
    logger.debug("USRP IN DONE MODE")
    done_mode_socket.send_string(HOSTNAME)
    done_reply = done_mode_socket.recv_string()
    logger.debug("DONE acknowledged: %s", done_reply)
    done_mode_socket.close()


def setup(usrp):
    rate = RATE
    mcr = 20e6
    assert (
        mcr / rate
    ).is_integer(), f"The masterclock rate {mcr} should be an integer multiple of the sampling rate {rate}"

    usrp.set_master_clock_rate(mcr)
    channels = [0, 1]
    setup_clock(usrp, "external", usrp.get_num_mboards())
    setup_pps(usrp, "external")

    rx_bw = 200e3
    for chan in channels:
        usrp.set_rx_rate(rate, chan)
        usrp.set_rx_dc_offset(True, chan)
        usrp.set_rx_bandwidth(rx_bw, chan)
        usrp.set_rx_agc(False, chan)

    usrp.set_rx_gain(REF_RX_GAIN, 0)
    usrp.set_rx_gain(LOOPBACK_RX_GAIN, 1)

    st_args = uhd.usrp.StreamArgs("fc32", "sc16")
    st_args.channels = channels
    rx_streamer = usrp.get_rx_stream(st_args)
    return rx_streamer


def rx_ref(
    usrp,
    rx_streamer,
    quit_event,
    duration,
    result_queue,
    capture_type,
    start_time=None,
):
    logger.debug("RX gains CH0=%s CH1=%s", usrp.get_rx_gain(0), usrp.get_rx_gain(1))

    num_channels = rx_streamer.get_num_channels()
    max_samps_per_packet = rx_streamer.get_max_num_samps()
    buffer_length = int(duration * RATE * 2)
    iq_data = np.empty((num_channels, buffer_length), dtype=np.complex64)

    recv_buffer = np.zeros((num_channels, max_samps_per_packet), dtype=np.complex64)
    rx_md = uhd.types.RXMetadata()
    stream_cmd = uhd.types.StreamCMD(uhd.types.StreamMode.start_cont)
    stream_cmd.stream_now = False

    timeout = 1.0
    if start_time is not None:
        stream_cmd.time_spec = start_time
        time_diff = start_time.get_real_secs() - usrp.get_time_now().get_real_secs()
        if time_diff > 0:
            timeout = 1.0 + time_diff
    else:
        stream_cmd.time_spec = uhd.types.TimeSpec(
            usrp.get_time_now().get_real_secs() + INIT_DELAY + 0.1
        )

    rx_streamer.issue_stream_cmd(stream_cmd)
    num_rx = 0
    result_pushed = False
    logged_metadata_errors = set()
    buffer_overflow_logged = False
    try:
        while not quit_event.is_set():
            try:
                num_rx_i = rx_streamer.recv(recv_buffer, rx_md, timeout)
                if rx_md.error_code != uhd.types.RXMetadataErrorCode.none:
                    logger.error(rx_md.error_code)
                    error_name = str(rx_md.error_code)
                    if error_name not in logged_metadata_errors:
                        append_error_log(
                            "RX_METADATA_ERROR",
                            f"RX metadata error during {capture_type}: {error_name}",
                            capture_type=capture_type,
                            metadata_error=error_name,
                        )
                        logged_metadata_errors.add(error_name)
                    continue
                if num_rx_i <= 0:
                    continue

                samples = recv_buffer[:, :num_rx_i]
                if num_rx + num_rx_i > buffer_length:
                    logger.error(
                        "More samples received than buffer length, not storing extra data."
                    )
                    if not buffer_overflow_logged:
                        append_error_log(
                            "RX_BUFFER_OVERFLOW",
                            f"RX buffer overflow during {capture_type}.",
                            capture_type=capture_type,
                            buffer_length=buffer_length,
                            num_rx=num_rx,
                            num_rx_i=num_rx_i,
                        )
                        buffer_overflow_logged = True
                    continue
                iq_data[:, num_rx : num_rx + num_rx_i] = samples
                num_rx += num_rx_i
            except RuntimeError as ex:
                error_msg = f"Runtime error in receive ({capture_type}): {ex}"
                logger.error(error_msg)
                append_error_log(
                    "RX_RUNTIME_ERROR",
                    error_msg,
                    capture_type=capture_type,
                )
                result_queue.put(
                    {
                        "ok": False,
                        "capture_type": capture_type,
                        "phase": None,
                        "amplitude": None,
                        "error": error_msg,
                    }
                )
                result_pushed = True
                return
    except KeyboardInterrupt:
        pass
    finally:
        logger.debug("Capture stop requested, stopping RX stream.")
        rx_streamer.issue_stream_cmd(uhd.types.StreamCMD(uhd.types.StreamMode.stop_cont))
        if result_pushed:
            return

        iq_capture = iq_data[:, :num_rx].copy()
        if num_rx <= 0:
            error_msg = f"No IQ samples captured during {capture_type}."
            logger.error(error_msg)
            append_error_log(
                "NO_IQ_SAMPLES",
                error_msg,
                capture_type=capture_type,
            )
            result_queue.put(
                {
                    "ok": False,
                    "capture_type": capture_type,
                    "phase": None,
                    "amplitude": None,
                    "error": error_msg,
                }
            )
            return

        analysis_start = int(RATE * 1)
        if num_rx > analysis_start:
            iq_samples = iq_data[:, analysis_start:num_rx]
        else:
            logger.warning(
                "Capture %s shorter than analysis window; using full capture for phase/amplitude summary.",
                capture_type,
            )
            append_error_log(
                "SHORT_CAPTURE_WINDOW",
                f"Capture {capture_type} shorter than analysis window; using full capture.",
                capture_type=capture_type,
                captured_samples=num_rx,
                analysis_start=analysis_start,
            )
            iq_samples = iq_capture

        try:
            phase_ch0, freq_slope_ch0_before, freq_slope_ch0_after = (
                tools.get_phases_and_apply_bandpass(iq_samples[0, :])
            )
            phase_ch1, freq_slope_ch1_before, freq_slope_ch1_after = (
                tools.get_phases_and_apply_bandpass(iq_samples[1, :])
            )
        except Exception as ex:
            error_msg = f"IQ analysis failed for {capture_type}: {ex}"
            logger.error(error_msg)
            append_error_log(
                "IQ_ANALYSIS_FAILED",
                error_msg,
                capture_type=capture_type,
            )
            result_queue.put(
                {
                    "ok": False,
                    "capture_type": capture_type,
                    "phase": None,
                    "amplitude": None,
                    "error": error_msg,
                }
            )
            return

        logger.debug(
            "Frequency offset CH0: %.2f Hz %.2f Hz",
            float(freq_slope_ch0_before),
            float(freq_slope_ch0_after),
        )
        logger.debug(
            "Frequency offset CH1: %.2f Hz %.2f Hz",
            float(freq_slope_ch1_before),
            float(freq_slope_ch1_after),
        )

        phase_diff = tools.to_min_pi_plus_pi(phase_ch0 - phase_ch1, deg=False)
        logger.debug(
            "Phase diff mean %s%s min %s%s max %s%s",
            fmt(np.rad2deg(tools.circmean(phase_diff, deg=False))),
            DEG,
            fmt(np.rad2deg(phase_diff).min()),
            DEG,
            fmt(np.rad2deg(phase_diff).max()),
            DEG,
        )

        phase_value = float(tools.circmean(phase_diff, deg=False))
        avg_ampl = np.mean(np.abs(iq_samples), axis=1)
        rms_ampl = np.sqrt(np.mean(np.abs(iq_samples) ** 2, axis=1))
        max_i = np.max(np.abs(np.real(iq_samples)), axis=1)
        max_q = np.max(np.abs(np.imag(iq_samples)), axis=1)
        amplitude_value = float(rms_ampl[1])

        logger.debug(
            "MAX AMPL IQ CH0: I %s Q %s CH1: I %s Q %s",
            fmt(max_i[0]),
            fmt(max_q[0]),
            fmt(max_i[1]),
            fmt(max_q[1]),
        )
        logger.debug(
            "AVG AMPL IQ CH0: %s CH1: %s",
            fmt(avg_ampl[0]),
            fmt(avg_ampl[1]),
        )

        if not np.isfinite(phase_value) or not np.isfinite(amplitude_value):
            error_msg = (
                f"Non-finite measurement summary for {capture_type}: "
                f"phase={phase_value}, amplitude={amplitude_value}"
            )
            logger.error(error_msg)
            append_error_log(
                "NONFINITE_MEASUREMENT",
                error_msg,
                capture_type=capture_type,
                phase=phase_value,
                amplitude=amplitude_value,
            )
            result_queue.put(
                {
                    "ok": False,
                    "capture_type": capture_type,
                    "phase": None,
                    "amplitude": None,
                    "error": error_msg,
                }
            )
            return

        max_peak = np.concatenate((max_i, max_q))
        if np.any(max_peak > PEAK_AMPLITUDE_HIGH_THRESHOLD):
            append_error_log(
                "HIGH_PEAK_AMPLITUDE",
                f"Peak IQ component above {PEAK_AMPLITUDE_HIGH_THRESHOLD} during {capture_type}.",
                capture_type=capture_type,
                threshold=PEAK_AMPLITUDE_HIGH_THRESHOLD,
                max_i=max_i,
                max_q=max_q,
            )

        if np.any(max_peak < PEAK_AMPLITUDE_LOW_THRESHOLD):
            append_error_log(
                "LOW_PEAK_AMPLITUDE",
                f"Peak IQ component below {PEAK_AMPLITUDE_LOW_THRESHOLD} during {capture_type}.",
                capture_type=capture_type,
                threshold=PEAK_AMPLITUDE_LOW_THRESHOLD,
                max_i=max_i,
                max_q=max_q,
            )

        result_queue.put(
            {
                "ok": True,
                "capture_type": capture_type,
                "phase": phase_value,
                "amplitude": amplitude_value,
                "avg_amplitude_ch0": float(avg_ampl[0]),
                "avg_amplitude_ch1": float(avg_ampl[1]),
                "rms_amplitude_ch0": float(rms_ampl[0]),
                "rms_amplitude_ch1": amplitude_value,
                "max_i_ch0": float(max_i[0]),
                "max_i_ch1": float(max_i[1]),
                "max_q_ch0": float(max_q[0]),
                "max_q_ch1": float(max_q[1]),
                "freq_offset_ch0_before_hz": float(freq_slope_ch0_before),
                "freq_offset_ch0_after_hz": float(freq_slope_ch0_after),
                "freq_offset_ch1_before_hz": float(freq_slope_ch1_before),
                "freq_offset_ch1_after_hz": float(freq_slope_ch1_after),
                "captured_samples": int(num_rx),
                "error": "",
            }
        )


def rx_thread(
    usrp,
    rx_streamer,
    quit_event,
    duration,
    res,
    capture_type,
    start_time=None,
):
    _rx_thread = threading.Thread(
        target=rx_ref,
        args=(
            usrp,
            rx_streamer,
            quit_event,
            duration,
            res,
            capture_type,
            start_time,
        ),
    )
    _rx_thread.name = "RX_thread"
    _rx_thread.start()
    return _rx_thread


def delta(usrp, at_time):
    return at_time - usrp.get_time_now().get_real_secs()


def starting_in(usrp, at_time):
    return f"Starting in {delta(usrp, at_time):.2f}s"


def measure_pilot(usrp, rx_streamer, quit_event, result_queue, at_time=None):
    logger.debug("########### Measure PILOT RX ###########")

    usrp.set_rx_antenna("TX/RX", 0)
    usrp.set_rx_antenna("TX/RX", 1)

    start_time = uhd.types.TimeSpec(at_time)
    logger.debug(starting_in(usrp, at_time))

    rx_thr = rx_thread(
        usrp,
        rx_streamer,
        quit_event,
        duration=CAPTURE_TIME,
        res=result_queue,
        capture_type="pilot",
        start_time=start_time,
    )

    time.sleep(CAPTURE_TIME + delta(usrp, at_time))

    quit_event.set()
    rx_thr.join()
    quit_event.clear()

    try:
        return result_queue.get(timeout=1.0)
    except queue.Empty:
        return {
            "ok": False,
            "capture_type": "pilot",
            "phase": None,
            "amplitude": None,
            "error": "Pilot result missing from queue.",
        }


def parse_arguments():
    global SERVER_IP

    parser = argparse.ArgumentParser(description="Uncalibrated pilot RX capture")
    parser.add_argument(
        "-i",
        "--ip",
        type=str,
        help="IP address of the server (optional)",
        required=False,
    )
    parser.add_argument("--config-file", type=str)

    args = parser.parse_args()

    if args.ip:
        logger.debug("Setting server IP to: %s", args.ip)
        SERVER_IP = args.ip
    return args


def main():
    global meas_id, exp_id, data_file

    args = parse_arguments()
    quit_event = None
    sync_config = None

    try:
        runtime_config = runtime_storage.resolve_runtime_output_dir(
            args.config_file,
            HOSTNAME,
        )
        sync_config = runtime_storage.resolve_rf_sync_endpoint(args.config_file)
        configure_file_logging(runtime_config["host_output_dir"])
        logger.info("Invocation args: %s", " ".join(sys.argv))
        logger.info(
            "Loaded experiment settings from %s",
            runtime_config["settings_path"],
        )
        logger.info("Runtime output directory: %s", RUNTIME_OUTPUT_DIR)
    except Exception as e:
        logger.error("Unable to initialize runtime storage: %s", e)
        return

    try:
        with open(os.path.join(os.path.dirname(__file__), "cal-settings.yml"), "r") as file:
            vars = yaml.safe_load(file)
            globals().update(vars)
    except FileNotFoundError:
        logger.error(
            "Calibration file 'cal-settings.yml' not found in the current directory."
        )
        exit()
    except yaml.YAMLError as e:
        logger.error("Error parsing 'cal-settings.yml': %s", e)
        exit()
    except Exception as e:
        logger.error("Unexpected error while loading calibration settings: %s", e)
        exit()

    if sync_config is not None:
        globals().update(
            {
                "SERVER_IP": sync_config["host"],
                "SYNC_PORT": sync_config["sync_port"],
                "ALIVE_PORT": sync_config["alive_port"],
                "DONE_PORT": sync_config["done_port"],
            }
        )
        logger.info(
            "RF sync endpoint: %s (sync=%s alive=%s done=%s)",
            SERVER_IP,
            SYNC_PORT,
            ALIVE_PORT,
            DONE_PORT,
        )

    try:
        script_dir = os.path.dirname(os.path.realpath(__file__))
        fpga_path = os.path.join(script_dir, "usrp_b210_fpga_loopback.bin")

        usrp = uhd.usrp.MultiUSRP(
            "enable_user_regs, " f"fpga={fpga_path}, " "mode_n=integer"
        )
        logger.info("Using Device: %s", usrp.get_pp_string())

        rx_streamer = setup(usrp)

        while True:
            wait_till_go_from_server(SERVER_IP)

            logger.info("Setting device timestamp to 0...")
            usrp.set_time_unknown_pps(uhd.types.TimeSpec(0.0))
            logger.debug("[SYNC] Resetting time.")
            logger.info("RX GAIN PROFILE CH0: %s", usrp.get_rx_gain_names(0))
            logger.info("RX GAIN PROFILE CH1: %s", usrp.get_rx_gain_names(1))
            time.sleep(2)

            tune_usrp_rx_only(usrp, FREQ, [0, 1], at_time=3.0)
            logger.info(
                "USRP has been tuned and setup. (%s)",
                usrp.get_time_now().get_real_secs(),
            )

            quit_event = threading.Event()
            result_queue = queue.Queue()

            pilot_result = measure_pilot(
                usrp,
                rx_streamer,
                quit_event,
                result_queue,
                at_time=START_PILOT_RX,
            )

            if not pilot_result["ok"]:
                logger.error(
                    "Pilot capture failed, skipping result write for this run. Reason: %s",
                    pilot_result["error"],
                )
                append_error_log(
                    "PILOT_MEASUREMENT_FAILED",
                    pilot_result["error"],
                    capture_type="pilot",
                    experiment_id=exp_id,
                    cycle_id=meas_id,
                    file_name=file_name,
                )
                send_usrp_done_mode(SERVER_IP)
                continue

            logger.info(
                "Phase pilot reference signal: %s (rad) / %s%s",
                fmt(pilot_result["phase"]),
                fmt(np.rad2deg(pilot_result["phase"])),
                DEG,
            )
            logger.info("Pilot RMS amplitude: %s", fmt(pilot_result["amplitude"]))

            result_record = {
                "timestamp_utc": utc_now_iso(),
                "hostname": HOSTNAME,
                "file_name": file_name,
                "experiment_id": exp_id,
                "cycle_id": int(meas_id),
                "pilot_phase": float(pilot_result["phase"]),
                "pilot_phase_deg": float(np.rad2deg(pilot_result["phase"])),
                "pilot_amplitude": float(pilot_result["amplitude"]),
                "avg_amplitude_ch0": float(pilot_result["avg_amplitude_ch0"]),
                "avg_amplitude_ch1": float(pilot_result["avg_amplitude_ch1"]),
                "rms_amplitude_ch0": float(pilot_result["rms_amplitude_ch0"]),
                "rms_amplitude_ch1": float(pilot_result["rms_amplitude_ch1"]),
                "max_i_ch0": float(pilot_result["max_i_ch0"]),
                "max_i_ch1": float(pilot_result["max_i_ch1"]),
                "max_q_ch0": float(pilot_result["max_q_ch0"]),
                "max_q_ch1": float(pilot_result["max_q_ch1"]),
                "freq_offset_ch0_before_hz": float(pilot_result["freq_offset_ch0_before_hz"]),
                "freq_offset_ch0_after_hz": float(pilot_result["freq_offset_ch0_after_hz"]),
                "freq_offset_ch1_before_hz": float(pilot_result["freq_offset_ch1_before_hz"]),
                "freq_offset_ch1_after_hz": float(pilot_result["freq_offset_ch1_after_hz"]),
                "captured_samples": int(pilot_result["captured_samples"]),
            }
            try:
                append_result_record(result_record)
                logger.info("Appended pilot result to %s", data_file_path)
            except Exception as ex:
                logger.error("Failed to append pilot result: %s", ex)
                append_error_log(
                    "RESULT_WRITE_FAILED",
                    f"Failed to append pilot result: {ex}",
                    experiment_id=exp_id,
                    cycle_id=meas_id,
                    file_name=file_name,
                )

            print("DONE")

            send_usrp_done_mode(SERVER_IP)

    except Exception as e:
        logger.debug("Sending signal to stop!")
        logger.error(e)
        append_error_log(
            "UNEXPECTED_EXCEPTION",
            str(e),
            experiment_id=exp_id,
            cycle_id=meas_id,
        )
        if quit_event is not None:
            quit_event.set()
    finally:
        if data_file is not None and not data_file.closed:
            data_file.close()
        time.sleep(1)
        sys.exit(0)


if __name__ == "__main__":
    while 1:
        main()
