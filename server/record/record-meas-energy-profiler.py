# ****************************************************************************************** #
#                                       IMPORTS / PATHS                                      #
# ****************************************************************************************** #

import argparse
from time import sleep, time
from typing import Optional

from Positioner import PositionerClient
from TechtilePlotter.TechtilePlotter import TechtilePlotter
import atexit
import os
import signal
import sys

import numpy as np
import zmq

# ****************************************************************************************** #
#                                           CONFIG                                           #
# ****************************************************************************************** #

SAVE_EVERY = 60.0  # seconds
FOLDER = (
    "RANDOM-ABS-REFL-0"  # subfolder inside data/where to save measurement data
)
TIMESTAMP = round(time())
DEFAULT_DURATION = None  # seconds, override via CLI

# -------------------------------------------------
# Directory and file names
# -------------------------------------------------
server_dir = os.path.dirname(os.path.abspath(__file__))
project_dir = os.path.dirname(server_dir)

# -------------------------------------------------
# lib imports
# -------------------------------------------------
PROJECT_ROOT = os.path.dirname(project_dir)
sys.path.insert(0, PROJECT_ROOT)
from lib.ep import RFEP
from lib.yaml_utils import read_yaml_file

# -------------------------------------------------
# config file
# -------------------------------------------------
settings = read_yaml_file("experiment-settings.yaml")

# -------------------------------------------------
# Data directory
# -------------------------------------------------
save_dir = os.path.abspath(os.path.join(server_dir, "../../data", FOLDER))
os.makedirs(save_dir, exist_ok=True)

# ****************************************************************************************** #
#                                      INITIALIZATION                                        #
# ****************************************************************************************** #

parser = argparse.ArgumentParser(description="Record energy profiler measurements.")
parser.add_argument(
    "--duration",
    dest="duration",
    type=str,
    help="Stop recording after a duration (e.g. '3h', '30m', '45s').",
)
parser.add_argument(
    "--load-existing",
    action="store_true",
    help="Load latest *_positions.npy and *_values.npy from the save folder and plot them.",
)
args = parser.parse_args()

def _parse_duration(value: Optional[str]) -> Optional[float]:
    if not value:
        return None
    value = value.strip().lower()
    unit = value[-1]
    if unit in {"h", "m", "s"} and len(value) > 1:
        num = float(value[:-1])
        if unit == "h":
            return num * 3600.0
        if unit == "m":
            return num * 60.0
        return num
    return float(value)

max_duration = _parse_duration(args.duration) or DEFAULT_DURATION
positioner = PositionerClient(config=settings["positioning"], backend="zmq")
rfep = RFEP(settings["ep"]["ip"], settings["ep"]["port"])

context = zmq.Context()
iq_socket = context.socket(zmq.PUB)
iq_socket.bind("tcp://*:50001")

plt = TechtilePlotter(realtime=True)

positions = []
values = []
last_save = 0
stop_requested = False


def save_data():
    """Safely save measurement data to disk."""
    print("Saving data...")
    positions_snapshot = list(positions)
    values_snapshot = list(values)

    if len(positions_snapshot) != len(values_snapshot):
        print(
            "Warning: positions and values length mismatch:",
            len(positions_snapshot),
            len(values_snapshot),
        )

    positions_path = os.path.join(save_dir, f"{TIMESTAMP}_positions.npy")
    values_path = os.path.join(save_dir, f"{TIMESTAMP}_values.npy")

    _atomic_save_npy(positions_path, positions_snapshot)
    _atomic_save_npy(values_path, values_snapshot)
    print("Data saved.")


def _atomic_save_npy(final_path, data):
    """Write to a temp file, fsync, then replace to avoid partial writes."""
    tmp_path = f"{final_path}.tmp"
    try:
        with open(tmp_path, "wb") as f:
            np.save(f, data)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, final_path)
    except Exception:
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except Exception:
            pass
        raise


def _handle_signal(signum, frame):
    global stop_requested
    stop_requested = True


def _save_data_safe():
    try:
        save_data()
    except Exception as e:
        print("Failed to save data on exit:", e)


atexit.register(_save_data_safe)
signal.signal(signal.SIGTERM, _handle_signal)


def _load_all_snapshots(folder_path):
    positions_files = sorted(
        [f for f in os.listdir(folder_path) if f.endswith("_positions.npy")]
    )
    values_files = sorted(
        [f for f in os.listdir(folder_path) if f.endswith("_values.npy")]
    )
    if not positions_files or not values_files:
        return []
    pos_map = {
        name[: -len("_positions.npy")]: os.path.join(folder_path, name)
        for name in positions_files
    }
    val_map = {
        name[: -len("_values.npy")]: os.path.join(folder_path, name)
        for name in values_files
    }
    bases = sorted(set(pos_map) & set(val_map))
    return [(pos_map[b], val_map[b]) for b in bases]


def _load_existing_data():
    pairs = _load_all_snapshots(save_dir)
    if not pairs:
        print("No existing position/value snapshots found to load.")
        return
    total = 0
    for pos_path, val_path in pairs:
        try:
            existing_positions = np.load(pos_path, allow_pickle=True).tolist()
            existing_values = np.load(val_path, allow_pickle=True).tolist()
        except Exception as exc:
            print(f"Failed to load existing snapshots {pos_path}, {val_path}: {exc}")
            continue
        if len(existing_positions) != len(existing_values):
            print(
                "Warning: existing positions and values length mismatch:",
                len(existing_positions),
                len(existing_values),
            )
        positions.extend(existing_positions)
        values.extend(existing_values)
        for pos, d in zip(existing_positions, existing_values):
            plt.measurements_rt(
                pos.x,
                pos.y,
                pos.z,
                d.pwr_pw / 1e6
            )
        total += len(existing_positions)
    print(f"Loaded {total} existing samples from {save_dir}.")


# ****************************************************************************************** #
#                                           MAIN                                             #
# ****************************************************************************************** #

try:
    print("Starting positioner and RFEP...")
    positioner.start()
    if args.load_existing:
        _load_existing_data()

    start_time = time()

    while True:
        d = rfep.get_data()
        pos = positioner.get_data()

        # print(d, pos)

        if d is not None and pos is not None:
            positions.append(pos)
            values.append(d)

            plt.measurements_rt(
                pos.x,
                pos.y,
                pos.z,
                d.pwr_pw / 1e6  # µW
            )
            print("x", end="", flush=True)
        else:
            print(".", end="", flush=True)

        # Periodic autosave
        if time() - last_save >= SAVE_EVERY:
            _save_data_safe()
            last_save = time()

        sleep(0.1)
        if max_duration is not None and time() - start_time >= max_duration:
            print(f"Reached configured duration ({max_duration:.0f} s). Stopping.")
            break
        if stop_requested:
            print("Stop requested. Exiting loop...")
            break

except KeyboardInterrupt:
    print("\nCtrl+C received. Stopping measurement...")

except Exception as e:
    print("Unexpected error:", e)
    raise

finally:
    # ****************************************************************************************** #
    #                                           CLEANUP                                          #
    # ****************************************************************************************** #
    print("Cleaning up...")

    _save_data_safe()

    try:
        positioner.stop()
    except Exception:
        pass

    try:
        rfep.stop()
    except Exception:
        pass

    iq_socket.close()
    context.term()

    print("Shutdown complete.")
    sys.exit(0)
