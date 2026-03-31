#!/usr/bin/env python3
"""
zmqclient_rover.py

ZMQ DEALER client for rover movements (XY plotter / GRBL-based robot).
Designed to work with zmq_orchestrator.py (ROUTER server).

Role: rover

Protocol:
  server -> MOVE      {experiment_id, cycle_id, meas_id, ts}
  client -> MOVE_DONE {experiment_id, cycle_id, meas_id, status="ok"}
          | MOVE_DONE {experiment_id, cycle_id, meas_id, status="error", error=<str>}

All movement parameters are read exclusively from config.yaml.
The server sends no parameters; messages carry only coordination identifiers.

config.yaml layout:
  serial_port: /dev/ttyUSB0
  baudrate: 115200
  work_area:
    width: 1250.0
    height: 1250.0
    margin: 10.0
  feed_rate: 4000.0
  grid:
    x_start: 300.0
    y_start: 300.0
    x_end:   900.0
    y_end:   900.0
    sweeps:        5       # total number of sweeps to generate
    start_spacing: 120.0   # spacing (mm) used for sweep 1
    decay:         0.75    # multiply spacing by this every odd sweep (>=3)
    min_spacing:   20.0    # floor for spacing
    start_sweep:   1       # resume from this sweep number (default 1)
  cycle_positions: true
  home_after_move: false
  verbose: false

Usage:
  python zmqclient_rover.py
  python zmqclient_rover.py --config-file config.yaml
  python zmqclient_rover.py --connect tcp://SERVER:5555 --config-file config.yaml
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import signal
import socket
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import zmq
import yaml

CLIENT_ROOT = Path(__file__).resolve().parents[1]
if str(CLIENT_ROOT) not in sys.path:
    sys.path.insert(0, str(CLIENT_ROOT))

import runtime_storage
from rover import WorkArea, XYPlotter

CLIENT_ID = "rover"
HOSTNAME_RAW = socket.gethostname()
HOSTNAME = HOSTNAME_RAW[4:] if len(HOSTNAME_RAW) > 4 else HOSTNAME_RAW

DEFAULT_CONFIG_PATH = Path(__file__).parent / "config.yaml"
DEFAULT_EXPERIMENT_SETTINGS_PATH = Path(__file__).resolve().parents[2] / "experiment-settings.yaml"
RUNTIME_OUTPUT_DIR = None
log_file_handler = None


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


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.propagate = False

formatter = LogFormatter(
    fmt="[%(asctime)s] [%(levelname)s] (%(threadName)-10s) %(message)s"
)

if not logger.handlers:
    console = logging.StreamHandler()
    console.setFormatter(ColoredFormatter(fmt=formatter._fmt))
    logger.addHandler(console)
    
    # --- Automatically save absolutely everything to a local log file ---
    local_file = logging.FileHandler("zmqclient_rover.log", mode="a")
    local_file.setFormatter(formatter)
    logger.addHandler(local_file)
    # -------------------------------------------------------------------------

def now_ms() -> int:
    return int(time.time() * 1000)


def jdump(obj: Dict[str, Any]) -> bytes:
    return json.dumps(obj, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def jload(b: bytes) -> Dict[str, Any]:
    return json.loads(b.decode("utf-8"))


def load_experiment_settings(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Experiment settings not found: {path}")
    with open(path, "r", encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}


def resolve_orchestrator_connect(connect: Optional[str], settings_path: Path) -> str:
    if connect:
        return connect

    settings = load_experiment_settings(settings_path)
    server_settings = settings.get("server") or {}
    host = server_settings.get("host")
    port = server_settings.get("orchestrator_port", 5555)

    if not host:
        raise ValueError(
            "Missing server.host in experiment settings; "
            "set server.host or pass --connect."
        )
    if "://" in str(host):
        return str(host)
    return f"tcp://{host}:{port}"


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

def load_config(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Rover config not found: {path}")
    with open(path) as f:
        cfg = yaml.safe_load(f)
    _validate_config(cfg)
    return cfg


def _validate_config(cfg: Dict[str, Any]) -> None:
    if "serial_port" not in cfg:
        raise ValueError("config must contain 'serial_port'")

    grid = cfg.get("grid")
    if not grid:
        raise ValueError("config must contain a 'grid' section")

    for key in ("x_start", "y_start", "x_end", "y_end"):
        if key not in grid:
            raise ValueError(f"config.grid must contain '{key}'")

    # At least one of the sweep or legacy spacing keys must be present.
    has_sweep  = "sweeps" in grid
    has_legacy = "spacing" in grid
    if not has_sweep and not has_legacy:
        raise ValueError(
            "config.grid must contain either 'sweeps' (sweep mode) "
            "or 'spacing' (legacy grid mode)"
        )

    if has_sweep and int(grid["sweeps"]) < 1:
        raise ValueError("grid.sweeps must be >= 1")

    if has_legacy and float(grid["spacing"]) <= 0:
        raise ValueError("grid.spacing must be positive")


def _compute_spacing_for_sweep(
    start_spacing: float,
    min_spacing: float,
    decay: float,
    target_sweep: int,
) -> float:
    """Return the spacing that would be active at *target_sweep*."""
    spacing = start_spacing
    for s in range(1, target_sweep):
        if s > 1 and s % 2 == 1:
            spacing = max(min_spacing, spacing * decay)
    return spacing


def _generate_sweep_points(
    sweeps: int,
    x_min: float,
    x_max: float,
    y_min: float,
    y_max: float,
    start_sweep: int = 1,
    start_spacing: float = 120.0,
    decay: float = 0.75,
    min_spacing: float = 20.0,
) -> list[tuple[int, list[tuple[float, float]]]]:
    """
    Generate all sweep paths without touching hardware.

    Returns a list of (sweep_number, [(x, y), ...]) tuples.

    Sweep strategy
    --------------
    - Even sweeps  → vertical scan lines (constant X, varying Y).
    - Odd sweeps   → horizontal scan lines (constant Y, varying X).
    - Spacing decays by *decay* on every odd sweep >= 3.
    - A half-spacing offset is applied to X lines on even sweeps and to
      Y lines on sweeps divisible by 3, spreading coverage across runs.
    - Each sweep starts at the area centre so the plotter approaches from
      a known position.
    - Lines are traversed in serpentine order to minimise travel distance.
    """
    all_sweeps: list[tuple[int, list[tuple[float, float]]]] = []
    spacing = _compute_spacing_for_sweep(start_spacing, min_spacing, decay, start_sweep)
    center  = ((x_min + x_max) / 2.0, (y_min + y_max) / 2.0)

    sweep_number = start_sweep
    for _ in range(sweeps):
        # Decay spacing on every odd sweep after the first.
        if sweep_number > 1 and sweep_number % 2 == 1 and sweep_number != start_sweep:
            spacing = max(min_spacing, spacing * decay)

        x_offset = (spacing / 2.0) if sweep_number % 2 == 0 else 0.0
        y_offset = (spacing / 2.0) if sweep_number % 3 == 0 else 0.0

        x_lines = np.arange(x_min + x_offset, x_max, spacing)
        y_lines = np.arange(y_min + y_offset, y_max, spacing)

        # Fall back to un-offset lines if the offset pushed us past the boundary.
        if len(x_lines) == 0:
            x_lines = np.arange(x_min, x_max, spacing)
        if len(y_lines) == 0:
            y_lines = np.arange(y_min, y_max, spacing)

        pts: list[tuple[float, float]] = [center]

        if sweep_number % 2 == 0:
            # Vertical scan lines along X.
            for idx, x_val in enumerate(x_lines):
                if idx % 2 == 0:
                    pts.append((float(x_val), y_min))
                    pts.append((float(x_val), y_max))
                else:
                    pts.append((float(x_val), y_max))
                    pts.append((float(x_val), y_min))
        else:
            # Horizontal scan lines along Y.
            for idx, y_val in enumerate(y_lines):
                if idx % 2 == 0:
                    pts.append((x_min, float(y_val)))
                    pts.append((x_max, float(y_val)))
                else:
                    pts.append((x_max, float(y_val)))
                    pts.append((x_min, float(y_val)))

        all_sweeps.append((sweep_number, pts))
        sweep_number += 1

    return all_sweeps


def build_positions_from_sweeps(cfg: Dict[str, Any]) -> List[Tuple[float, float]]:
    """
    Read sweep parameters from *cfg* and return a flat, ordered list of
    (x, y) waypoints — a drop-in replacement for the old build_grid().

    All sweep paths are concatenated in sweep order; the caller can index
    into the list with move_counter just as before.
    """
    grid = cfg["grid"]

    x_min = float(grid["x_start"])
    y_min = float(grid["y_start"])
    x_max = float(grid["x_end"])
    y_max = float(grid["y_end"])

    sweeps        = int(grid.get("sweeps",        5))
    start_sweep   = int(grid.get("start_sweep",   1))
    start_spacing = float(grid.get("start_spacing", 120.0))
    decay         = float(grid.get("decay",        0.75))
    min_spacing   = float(grid.get("min_spacing",  20.0))

    sweep_data = _generate_sweep_points(
        sweeps        = sweeps,
        x_min         = x_min,
        x_max         = x_max,
        y_min         = y_min,
        y_max         = y_max,
        start_sweep   = start_sweep,
        start_spacing = start_spacing,
        decay         = decay,
        min_spacing   = min_spacing,
    )

    positions: List[Tuple[float, float]] = []
    for sweep_number, pts in sweep_data:
        positions.extend(pts)

    return positions

def run_rover(
    x: float,
    y: float,
    cfg: Dict[str, Any],
    move_counter: int,
) -> None:
    """Move the plotter to (x, y) using settings from cfg."""
    wa_cfg = cfg.get("work_area", {})
    area   = WorkArea(
        width  = float(wa_cfg.get("width",  1250.0)),
        height = float(wa_cfg.get("height", 1250.0)),
        margin = float(wa_cfg.get("margin",   10.0)),
    )
    feed_rate       = float(cfg.get("feed_rate", 4000.0))
    home_after_move = bool(cfg.get("home_after_move", False))
    port            = cfg["serial_port"]
    baudrate        = int(cfg.get("baudrate", 115200))

    clamped_x, clamped_y = area.clamp(x, y)
    if (clamped_x, clamped_y) != (x, y):
        logger.warning(
            f"[{CLIENT_ID}] WARNING: target ({x:.3f}, {y:.3f}) outside work area "
            f"– clamped to ({clamped_x:.3f}, {clamped_y:.3f})"
        )
        x, y = clamped_x, clamped_y

    logger.info(
        "[%s] moving to X=%.3f Y=%.3f feed=%s port=%s",
        CLIENT_ID,
        x,
        y,
        feed_rate,
        port,
    )
    t_start = time.time()

    with XYPlotter(port, baudrate=baudrate) as plotter:
        if move_counter == 1:
            plotter.home()
        plotter.move(x, y, feed_rate=feed_rate)
        if home_after_move:
            plotter.move_to_origin()

    logger.info("[%s] move complete in %.3f s", CLIENT_ID, time.time() - t_start)

def rover_client(connect: str, config_path: Path) -> None:
    cfg: Dict[str, Any]  = load_config(config_path)
    cycle_positions: bool = cfg.get("cycle_positions", True)

    positions = build_positions_from_sweeps(cfg)
    logger.info("[%s] loaded config from %s", CLIENT_ID, config_path)
    logger.info(
        "[%s] sweep grid: %d total waypoint(s) across %d sweep(s)",
        CLIENT_ID,
        len(positions),
        cfg["grid"].get("sweeps", 5),
    )
    if cfg.get("verbose"):
        for i, (x, y) in enumerate(positions):
            logger.info("  [%4d] X=%.3f Y=%.3f", i, x, y)

    ctx  = zmq.Context.instance()
    sock = ctx.socket(zmq.DEALER)
    sock.linger = 0
    sock.setsockopt(zmq.IDENTITY, CLIENT_ID.encode("utf-8"))
    sock.connect(connect)

    poller = zmq.Poller()
    poller.register(sock, zmq.POLLIN)

    stop         = {"flag": False}
    move_counter = 0

    def _sigint(_sig, _frame):
        stop["flag"] = True

    signal.signal(signal.SIGINT, _sigint)

    def send(msg: Dict[str, Any]) -> None:
        sock.send(jdump(msg))

    def recv(timeout_ms: int = 1000) -> Optional[Dict[str, Any]]:
        events = dict(poller.poll(timeout_ms))
        if sock not in events:
            return None
        return jload(sock.recv())

    send({"type": "HELLO", "id": CLIENT_ID, "ts": now_ms()})
    logger.info("[%s] connected to %s. Waiting for commands...", CLIENT_ID, connect)

    while not stop["flag"]:
        msg = recv(timeout_ms=1000)
        if msg is None:
            continue

        mtype         = msg.get("type")
        experiment_id = msg.get("experiment_id")
        cycle_id      = msg.get("cycle_id")
        meas_id       = msg.get("meas_id")

        if mtype == "MOVE":
            move_counter += 1

            if cycle_positions:
                pos_index = (move_counter - 1) % len(positions)
            else:
                pos_index = move_counter - 1
                if pos_index >= len(positions):
                    logger.info(
                        f"[{CLIENT_ID}][exp {experiment_id}][meas {meas_id}] "
                        f"ALL_DONE: sweep grid DONE after {len(positions)} waypoint(s)."
                    )
                    send({
                        "type":          "MOVE_DONE",
                        "experiment_id": experiment_id,
                        "cycle_id":      cycle_id,
                        "meas_id":       meas_id,
                        "id":            CLIENT_ID,
                        "status":        "ALL_DONE",
                        "ts":            now_ms(),
                    })
                    continue

            x, y = positions[pos_index]

            logger.info(
                f"[{CLIENT_ID}][exp {experiment_id}][meas {meas_id}] "
                f"MOVE received  → waypoint[{pos_index}] X={x:.3f}  Y={y:.3f}"
            )

            try:
                run_rover(x, y, cfg, move_counter)

                response: Dict[str, Any] = {
                    "type":          "MOVE_DONE",
                    "experiment_id": experiment_id,
                    "cycle_id":      cycle_id,
                    "meas_id":       meas_id,
                    "id":            CLIENT_ID,
                    "status":        "ok",
                    "ts":            now_ms(),
                }
                logger.info(
                    "[%s][exp %s][meas %s] MOVE_DONE",
                    CLIENT_ID,
                    experiment_id,
                    meas_id,
                )

            except Exception as exc:
                response = {
                    "type":          "MOVE_DONE",
                    "experiment_id": experiment_id,
                    "cycle_id":      cycle_id,
                    "meas_id":       meas_id,
                    "id":            CLIENT_ID,
                    "status":        "error",
                    "error":         str(exc),
                    "ts":            now_ms(),
                }
                logger.error(
                    "[%s][exp %s][meas %s] ERROR: %s",
                    CLIENT_ID,
                    experiment_id,
                    meas_id,
                    exc,
                )

            send(response)

        elif mtype == "PING":
            send({"type": "PONG", "id": CLIENT_ID, "ts": now_ms()})

        else:
            logger.warning(
                "[%s] unexpected message type '%s' - sending ERROR",
                CLIENT_ID,
                mtype,
            )
            send({
                "type":          "ERROR",
                "experiment_id": experiment_id,
                "cycle_id":      cycle_id,
                "meas_id":       meas_id,
                "id":            CLIENT_ID,
                "error":         f"Unexpected message type: {mtype}",
                "ts":            now_ms(),
            })

    logger.info("[%s] shutting down.", CLIENT_ID)
    sock.close()
    ctx.term()

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Rover movement ZMQ client")
    p.add_argument(
        "--connect",
        default=None,
        help="ZMQ endpoint of the orchestrator server; defaults to server.host from experiment-settings.yaml",
    )
    p.add_argument(
        "--experiment-settings",
        default=str(DEFAULT_EXPERIMENT_SETTINGS_PATH),
        help="Path to experiment-settings.yaml",
    )
    p.add_argument(
        "--config-file", default=str(DEFAULT_CONFIG_PATH),
        help="Path to config.yaml  (default: ./config.yaml)",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()

    try:
        runtime_config = runtime_storage.resolve_runtime_output_dir(
            args.experiment_settings,
            HOSTNAME,
        )
        configure_file_logging(runtime_config["host_output_dir"])
        logger.info("Invocation args: %s", " ".join(sys.argv))
        logger.info(
            "Loaded experiment settings from %s",
            runtime_config["settings_path"],
        )
        logger.info("Runtime output directory: %s", RUNTIME_OUTPUT_DIR)
    except Exception as exc:
        logger.error("Unable to initialize runtime storage: %s", exc)
        return

    connect = resolve_orchestrator_connect(
        args.connect,
        Path(args.experiment_settings),
    )
    logger.info("Orchestrator endpoint: %s", connect)
    try:
        rover_client(connect, Path(args.config_file))
    except Exception:
        logger.exception("rover client failed")
        raise


if __name__ == "__main__":
    main()