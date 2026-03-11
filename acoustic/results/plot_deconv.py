import csv
import ast
import json
import random
import numpy as np
import matplotlib.pyplot as plt
import pyroomacoustics as pra
from pathlib import Path


def simulate_rir(room_dim, source_pos, mic_pos, sample_rate, max_order=3):
    materials = {
        "west":    pra.Material(1.0),  # fully absorbing
        "east":    pra.Material(1.0),  # fully absorbing
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
    mic_array = np.array(mic_pos).reshape(3, 1)
    room.add_microphone(mic_array)
    room.compute_rir()
    return room.rir[0][0]


def plot_random_RIRs_with_sim(csv_filename: Path, config_filename: Path, n: int = 6, seed: int = None):
    csv.field_size_limit(10 * 1024 * 1024)

    with open(config_filename, "r") as f:
        config = json.load(f)

    sample_rate = config["sample_rate"]
    room_dim    = config["room_dim"]

    mic_rows = []

    with open(csv_filename, mode="r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row["values"] == "unused":
                continue
            if row["microphone_label"] == "chirp_excitation":
                continue
            rir          = np.array(ast.literal_eval(row["values"]))
            coords_tuple = ast.literal_eval(row["microphone_coordinates"])
            
            speaker_pos  = ast.literal_eval(row["speaker"])
            mic_rows.append({
                "mic_id":  row["microphone_label"],
                "coords":  coords_tuple,
                "speaker": speaker_pos,
                "rir":     rir,
            })

    if not mic_rows:
        raise ValueError("No usable microphone rows found in the CSV.")

    if len(mic_rows) < n:
        print(f"Only {len(mic_rows)} usable mics found — plotting all.")
        n = len(mic_rows)
    if seed is not None:
        random.seed(seed)
    selected = random.sample(mic_rows, n)

    print(f"Simulating {n} RIRs with pyroomacoustics...")
    for entry in selected:
        mic_pos     = list(entry["coords"])
        speaker_pos = entry["speaker"]
        entry["rir_sim"] = simulate_rir(room_dim, speaker_pos, mic_pos, sample_rate)
        print(f"  {entry['mic_id']} at {entry['coords']}  |  speaker at {speaker_pos}")

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

    speaker = selected[0]["speaker"]
    fig.suptitle(
        f"Measured vs Simulated RIRs (normalised) — Speaker: {speaker}\n"
        f"Room: {room_dim} m  |  Fs: {sample_rate / 1000:.0f} kHz",
        fontsize=11, fontweight="bold",
    )
    plt.tight_layout()
    plt.savefig("rir_comparison_deconv.png", dpi=1000)
    print("Saved!")
    plt.show()


BASE_DIR    = Path(__file__).resolve().parent
CSV_FILE    = BASE_DIR / "direct_rir/Measured_Signal_4.555_2.645_0.215_20260311_170818.csv"
CONFIG_FILE = BASE_DIR.parent / "config.json"

plot_random_RIRs_with_sim(
    csv_filename    = CSV_FILE,
    config_filename = CONFIG_FILE,
    n               = 6,
    seed            = 2,
)