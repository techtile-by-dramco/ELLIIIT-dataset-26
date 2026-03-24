from acousticMeasurement import run_acoustic_measurement
import csv
import ast
import json
import random
import numpy as np
import matplotlib.pyplot as plt
import pyroomacoustics as pra
from pathlib import Path
from scipy.signal import convolve


def calculateRIRDeconvolution(config: dict, RX_data: np.ndarray, chirpExcitation: np.ndarray) -> list:
    fs       = config["sample_rate"]
    duration = config["chirp_duration"]
    f1       = config["chirp_f_start"]
    f2       = config["chirp_f_stop"]
    amp      = config["chirp_ampl"]
    N        = len(chirpExcitation)
    L        = duration / np.log(f2 / f1)
    p        = np.arange(N) / fs
    weight   = (f1 / f2) * np.exp(+p / L)
    inv_filter = amp * np.flipud(chirpExcitation) * weight
    measured_RIRs = []
    for rx in RX_data:
        conv = convolve(rx, inv_filter, mode="full")
        rir  = conv[N - 1 : 2 * N - 1]
        measured_RIRs.append(rir)
    return measured_RIRs


def simulate_rir(room_dim, source_pos, mic_pos, sample_rate, max_order=3):
    materials = {
        "west":    pra.Material(1.0),
        "east":    pra.Material(1.0),
        "south":   pra.Material(0.3),
        "north":   pra.Material(0.3),
        "ceiling": pra.Material(0.3),
        "floor":   pra.Material(0.3),
    }
    room = pra.ShoeBox(
        room_dim,
        fs=sample_rate,
        materials=materials,
        max_order=max_order,
        air_absorption=True,
        temperature=25,
    )
    room.add_source(source_pos)
    room.add_microphone(np.array(mic_pos))
    room.compute_rir()
    return room.rir[0][0]


def plot_random_RIRs_with_sim(source_pos, csv_filename: Path, config_filename: Path, n: int = 6, seed: int = None):
    csv.field_size_limit(10 * 1024 * 1024)

    with open(config_filename, "r") as f:
        config = json.load(f)

    sample_rate = config["sample_rate"]
    room_dim    = config["room_dim"]

    mic_rows         = []
    chirp_excitation = None
    chirp_config     = None
    speaker_label    = None

    with open(csv_filename, mode="r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row["values"] == "unused":
                continue

            row_chirp_cfg = {
                "sample_rate":    sample_rate,
                "chirp_duration": float(row["duration"]),
                "chirp_f_start":  float(row["f_start"]),
                "chirp_f_stop":   float(row["f_stop"]),
                "chirp_ampl":     float(row["chirp_amp"]),
            }

            if row["microphone_label"] == "chirp_excitation":
                chirp_excitation = np.array(ast.literal_eval(row["values"]))
                chirp_config     = row_chirp_cfg
            else:
                ess          = np.array(ast.literal_eval(row["values"]))
                coords_tuple = ast.literal_eval(row["microphone_coordinates"])
                
                mic_rows.append({
                    "mic_id":       row["microphone_label"],
                    "coords":       coords_tuple,
                    "ess":          ess,
                    "chirp_config": row_chirp_cfg,
                })

    if chirp_excitation is None:
        raise ValueError("No 'chirp_excitation' row found in the CSV.")
    if chirp_config is None:
        raise ValueError("Could not read chirp parameters from the CSV.")
    if not mic_rows:
        raise ValueError("No usable microphone rows found in the CSV.")

    print(
        f"Chirp params from CSV: duration={chirp_config['chirp_duration']} s  "
        f"f1={chirp_config['chirp_f_start']} Hz  f2={chirp_config['chirp_f_stop']} Hz  "
        f"amp={chirp_config['chirp_ampl']}"
    )

    print(f"Deconvolving ESS for {len(mic_rows)} microphones …")

    from itertools import groupby

    def chirp_key(r):
        c = r["chirp_config"]
        return (c["chirp_duration"], c["chirp_f_start"], c["chirp_f_stop"], c["chirp_ampl"])

    sorted_rows = sorted(mic_rows, key=chirp_key)
    for _key, group in groupby(sorted_rows, key=chirp_key):
        group = list(group)
        cfg   = group[0]["chirp_config"]
        RX    = np.array([r["ess"] for r in group])
        rirs  = calculateRIRDeconvolution(cfg, RX, chirp_excitation)
        for row, rir in zip(group, rirs):
            row["rir"] = rir

    if len(mic_rows) < n:
        print(f"Only {len(mic_rows)} usable mics found — plotting all.")
        n = len(mic_rows)
    if seed is not None:
        random.seed(seed)
    selected = random.sample(mic_rows, n)

    print(f"Simulating {n} RIRs with pyroomacoustics …")
    for entry in selected:
        mic_pos    = list(entry["coords"])
        entry["rir_sim"] = simulate_rir(room_dim, source_pos, mic_pos, sample_rate, 5)
        print(f"  {entry['mic_id']} at {entry['coords']}")

    cols = 2
    rows = (n + 1) // cols
    fig, axes = plt.subplots(rows, cols, figsize=(16, rows * 4))
    axes = axes.flatten()

    for i, entry in enumerate(selected):
        ax = axes[i]

        meas = np.abs(entry["rir"])
        meas = meas / meas.max()
        t_meas = np.arange(len(meas)) / sample_rate * 1000

        L = len(meas)
        sim_full = np.abs(entry["rir_sim"])
        sim_full = sim_full / sim_full.max()
        sim = sim_full[:L] if len(sim_full) >= L else np.pad(sim_full, (0, L - len(sim_full)))
        t_sim = t_meas

        ax.plot(t_meas, meas, linewidth=0.7, color=f"C{i}", label="Measured",  alpha=0.85)
        ax.plot(t_sim,  sim,  linewidth=0.9, color="black", label="Simulated", alpha=0.7, linestyle="--")
        ax.set_title(f"Mic: {entry['mic_id']}  |  Coords: {entry['coords']}", fontsize=9)
        ax.set_xlabel("Time (ms)", fontsize=8)
        ax.set_ylabel("Normalised |Amplitude|", fontsize=8)
        ax.set_ylim(-0.05, 1.1)
        ax.tick_params(labelsize=7)
        ax.grid(True, alpha=0.3)
        ax.legend(fontsize=7, loc="upper right")

    for j in range(i + 1, len(axes)):
        axes[j].set_visible(False)

    fig.suptitle(
        f"Measured (deconvolved ESS) vs Simulated RIRs — Speaker: {mic_pos}\n"
        f"Room: {room_dim} m  |  Fs: {sample_rate / 1000:.0f} kHz",
        fontsize=11, fontweight="bold",
    )
    plt.tight_layout()
    plt.savefig("rir_comparison_ess.png", dpi=1000)
    print("Saved!")
    plt.show()


BASE_DIR    = Path(__file__).resolve().parent
CSV_FILE    = BASE_DIR / "results/Measured_Signal_20260324_140041.csv"
CONFIG_FILE = BASE_DIR / "config.json"

def main():
    print("Running acoustic measurement...")
    run_acoustic_measurement()
    print("\n--- Measurement Complete ---")

if __name__ == "__main__":
    main()
    plot_random_RIRs_with_sim(
    source_pos= [1.33, 2.47, 0.46],
    csv_filename    = CSV_FILE,
    config_filename = CONFIG_FILE,
    n               = 10,
    seed            = 4,
)