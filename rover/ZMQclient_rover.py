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

All movement parameters are read exclusively from rover_config.json.
The server sends no parameters; messages carry only coordination identifiers.

rover_config.json layout:
{
    "serial_port":       "COM3",
    "baudrate":          115200,
    "work_area":         { "width": 1250.0, "height": 1250.0, "margin": 10.0 },
    "feed_rate":         20.0,
    "positions":         [[100.0, 100.0], [300.0, 100.0], [300.0, 300.0]],
    "cycle_positions":   true,
    "home_after_move":   true,
    "verbose":           false
}

Usage:
  python zmqclient_rover.py --connect tcp://127.0.0.1:5555
  python zmqclient_rover.py --connect tcp://127.0.0.1:5555 --config rover_config.json
"""

from __future__ import annotations

import argparse
import json
import signal
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import zmq

from rover import WorkArea, XYPlotter

CLIENT_ID = "rover"

DEFAULT_CONFIG_PATH = Path(__file__).parent / "config.json"


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def now_ms() -> int:
    return int(time.time() * 1000)


def jdump(obj: Dict[str, Any]) -> bytes:
    return json.dumps(obj, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def jload(b: bytes) -> Dict[str, Any]:
    return json.loads(b.decode("utf-8"))


# ---------------------------------------------------------------------------
# config
# ---------------------------------------------------------------------------

def load_config(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Rover config not found: {path}")
    with open(path) as f:
        cfg = json.load(f)
    _validate_config(cfg)
    return cfg


def _validate_config(cfg: Dict[str, Any]) -> None:
    if "serial_port" not in cfg:
        raise ValueError("rover_config must contain 'serial_port'")
    positions = cfg.get("positions")
    if not positions:
        raise ValueError("rover_config must contain a non-empty 'positions' list")
    for pos in positions:
        if len(pos) != 2:
            raise ValueError(f"Each position must be [x, y], got: {pos}")


# ---------------------------------------------------------------------------
# movement
# ---------------------------------------------------------------------------

def run_rover(x: float, y: float, cfg: Dict[str, Any]) -> None:
    """Move the plotter to (x, y) using settings from cfg."""
    wa    = cfg.get("work_area", {})
    area  = WorkArea(
        width  = float(wa.get("width",  1250.0)),
        height = float(wa.get("height", 1250.0)),
        margin = float(wa.get("margin",   10.0)),
    )
    feed_rate       = float(cfg.get("feed_rate", 20.0))
    home_after_move = bool(cfg.get("home_after_move", True))
    port            = cfg["serial_port"]
    baudrate        = int(cfg.get("baudrate", 115200))

    clamped_x, clamped_y = area.clamp(x, y)
    if (clamped_x, clamped_y) != (x, y):
        print(
            f"[{CLIENT_ID}] WARNING: target ({x:.3f}, {y:.3f}) outside work area "
            f"– clamped to ({clamped_x:.3f}, {clamped_y:.3f})"
        )
        x, y = clamped_x, clamped_y

    print(f"[{CLIENT_ID}] moving to X={x:.3f}  Y={y:.3f}  feed={feed_rate}  port={port}")
    t_start = time.time()

    with XYPlotter(port, baudrate=baudrate) as plotter:
        plotter.home()
        plotter.move(x, y, feed_rate=feed_rate)
        if home_after_move:
            plotter.move_to_origin()

    print(f"[{CLIENT_ID}] move complete in {round(time.time() - t_start, 3)} s")


# ---------------------------------------------------------------------------
# ZMQ client
# ---------------------------------------------------------------------------

def rover_client(connect: str, config_path: Path) -> None:
    cfg: Dict[str, Any]         = load_config(config_path)
    positions: List[List[float]] = cfg["positions"]
    cycle_positions: bool        = cfg.get("cycle_positions", True)

    print(f"[{CLIENT_ID}] loaded config from {config_path}")
    print(f"[{CLIENT_ID}] {len(positions)} position(s) configured")

    ctx = zmq.Context.instance()
    sock = ctx.socket(zmq.DEALER)
    sock.linger = 0
    sock.setsockopt(zmq.IDENTITY, CLIENT_ID.encode("utf-8"))
    sock.connect(connect)

    poller = zmq.Poller()
    poller.register(sock, zmq.POLLIN)

    stop         = {"flag": False}
    move_counter = 0           # tracks how many MOVE commands have been received

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
    print(f"[{CLIENT_ID}] connected to {connect}. Waiting for commands…")

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
            pos_index = ((move_counter - 1) % len(positions)) if cycle_positions else 0
            x, y = float(positions[pos_index][0]), float(positions[pos_index][1])

            print(
                f"[{CLIENT_ID}][exp {experiment_id}][meas {meas_id}] "
                f"MOVE received  → position[{pos_index}] X={x}  Y={y}"
            )

            try:
                run_rover(x, y, cfg)

                response: Dict[str, Any] = {
                    "type":          "MOVE_DONE",
                    "experiment_id": experiment_id,
                    "cycle_id":      cycle_id,
                    "meas_id":       meas_id,
                    "id":            CLIENT_ID,
                    "status":        "ok",
                    "ts":            now_ms(),
                }
                print(f"[{CLIENT_ID}][exp {experiment_id}][meas {meas_id}] MOVE_DONE")

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
                print(f"[{CLIENT_ID}][exp {experiment_id}][meas {meas_id}] ERROR: {exc}")

            send(response)

        elif mtype == "PING":
            send({"type": "PONG", "id": CLIENT_ID, "ts": now_ms()})

        else:
            print(f"[{CLIENT_ID}] unexpected message type '{mtype}' — sending ERROR")
            send({
                "type":          "ERROR",
                "experiment_id": experiment_id,
                "cycle_id":      cycle_id,
                "meas_id":       meas_id,
                "id":            CLIENT_ID,
                "error":         f"Unexpected message type: {mtype}",
                "ts":            now_ms(),
            })

    print(f"[{CLIENT_ID}] shutting down.")
    sock.close()
    ctx.term()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Rover movement ZMQ client")
    p.add_argument(
        "--connect", default="tcp://127.0.0.1:5555",
        help="ZMQ endpoint of the orchestrator server",
    )
    p.add_argument(
        "--config", default=str(DEFAULT_CONFIG_PATH),
        help="Path to rover_config.json  (default: ./rover_config.json)",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    rover_client(args.connect, Path(args.config))


if __name__ == "__main__":
    main()