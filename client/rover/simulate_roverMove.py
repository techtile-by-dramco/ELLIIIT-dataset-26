#!/usr/bin/env python3
"""
run_rover.py

Standalone script to drive the XY plotter through a generated grid of positions.
Reads all parameters from rover_config.yaml (same file used by zmqclient_rover.py).

Usage:
  python run_rover.py
  python run_rover.py --config-file config.yaml
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import List, Tuple

import yaml

from rover import WorkArea, XYPlotter, run_rover

BASE_DIR     = Path(__file__).parent.resolve()
CONFIG_PATH  = BASE_DIR / "config.yaml"

def load_config(path: Path = CONFIG_PATH) -> dict:
    if not path.exists():
        raise FileNotFoundError(f"Rover config not found: {path}")
    with open(path) as f:
        cfg = yaml.safe_load(f)
    _validate_config(cfg)
    return cfg


def _validate_config(cfg: dict) -> None:
    if "serial_port" not in cfg:
        raise ValueError("rover_config must contain 'serial_port'")
    if cfg.get("feed_rate", 0) <= 0:
        raise ValueError("feed_rate must be > 0")

    grid = cfg.get("grid")
    if not grid:
        raise ValueError("rover_config must contain a 'grid' section")
    for key in ("x_start", "y_start", "x_end", "y_end", "spacing"):
        if key not in grid:
            raise ValueError(f"rover_config.grid must contain '{key}'")
    if float(grid["spacing"]) <= 0:
        raise ValueError("grid.spacing must be positive")

def build_grid(cfg: dict) -> List[Tuple[float, float]]:
    """
    Build a flat list of (x, y) grid nodes in serpentine order.
    Even rows (0-indexed) go left→right; odd rows go right→left.
    """
    grid  = cfg["grid"]
    x0    = float(grid["x_start"])
    y0    = float(grid["y_start"])
    x1    = float(grid["x_end"])
    y1    = float(grid["y_end"])
    step  = float(grid["spacing"])

    def axis(start: float, end: float) -> List[float]:
        lo, hi = (start, end) if start <= end else (end, start)
        values: List[float] = []
        v = lo
        while v <= hi + step * 1e-6:
            values.append(round(v, 6))
            v += step
        return values if start <= end else list(reversed(values))

    xs = axis(x0, x1)
    ys = axis(y0, y1)

    points: List[Tuple[float, float]] = []
    for row_idx, y in enumerate(ys):
        row_xs = xs if row_idx % 2 == 0 else list(reversed(xs))
        for x in row_xs:
            points.append((x, y))

    return points

def move_through_grid(cfg: dict) -> None:
    positions       = build_grid(cfg)
    cycle_positions = cfg.get("cycle_positions", False)
    home_after_move = cfg.get("home_after_move", False)

    total = len(positions)
    print(f"Grid has {total} node(s). cycle_positions={cycle_positions}")

    def run_sequence() -> None:
        for idx, (x, y) in enumerate(positions):
            print(f"→ [{idx + 1}/{total}] moving to ({x:.3f}, {y:.3f})")
            run_rover(x, y, cfg)
        if home_after_move:
            print("→ returning to origin after full sequence")
            run_rover(0.0, 0.0, cfg)

    run_sequence()

    if cycle_positions:
        print("\nCycling enabled — repeating sequence indefinitely (Ctrl-C to stop)…")
        while True:
            run_sequence()

def main() -> None:
    parser = argparse.ArgumentParser(description="Drive the XY plotter through a grid")
    parser.add_argument(
        "--config-file", default=str(CONFIG_PATH),
        help="Path to rover_config.yaml (default: ./config.yaml)",
    )
    args = parser.parse_args()

    cfg = load_config(Path(args.config))

    # Home once before any movement
    print("Homing plotter…")
    with XYPlotter(cfg["serial_port"], baudrate=cfg.get("baudrate", 115200)) as plotter:
        plotter.home()

    print("Running grid sweep…")
    move_through_grid(cfg)
    print("\n--- Sweep complete ---")


if __name__ == "__main__":
    main()