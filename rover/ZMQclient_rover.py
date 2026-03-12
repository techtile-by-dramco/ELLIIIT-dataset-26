#!/usr/bin/env python3
"""
zmqclient_rover.py

ZMQ DEALER client that performs rover movements (XY plotter / robot).
Designed to work with zmq_orchestrator.py (ROUTER server).

Role: rover

Protocol:
  server -> MOVE      {experiment_id, cycle_id, meas_id, <optional move params>}
  client -> MOVE_DONE {experiment_id, cycle_id, meas_id, status, x, y, duration_s}
          | MOVE_DONE {experiment_id, cycle_id, meas_id, status="error", error=<str>}

Optional movement parameters forwarded from the server in MOVE:
  x            : float  [mm]  target X position
  y            : float  [mm]  target Y position
  feed_rate    : float  [mm/min]

Usage:
  python zmqclient_rover.py --connect tcp://127.0.0.1:5555
"""

from __future__ import annotations

import argparse
import json
import signal
import time
from typing import Any, Dict, Optional

import zmq

from rover import load_config, run_rover

MOVE_PARAM_KEYS = [
    "speaker_coordinates",
    "feed_rate",
]

CLIENT_ID = "rover"


def now_ms() -> int:
    return int(time.time() * 1000)


def jdump(obj: Dict[str, Any]) -> bytes:
    return json.dumps(obj, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def jload(b: bytes) -> Dict[str, Any]:
    return json.loads(b.decode("utf-8"))


def rover_client(connect: str) -> None:
    base_config = load_config()
    if base_config.get("get_system_info"):
        read_system()

    ctx = zmq.Context.instance()
    sock = ctx.socket(zmq.DEALER)
    sock.linger = 0
    sock.setsockopt(zmq.IDENTITY, CLIENT_ID.encode("utf-8"))
    sock.connect(connect)

    poller = zmq.Poller()
    poller.register(sock, zmq.POLLIN)

    stop = {"flag": False}

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
    print(f"[{CLIENT_ID}] Connected to {connect}. Waiting for commands…")

    while not stop["flag"]:
        msg = recv(timeout_ms=1000)
        if msg is None:
            continue

        mtype         = msg.get("type")
        experiment_id = msg.get("experiment_id")
        cycle_id      = msg.get("cycle_id")
        meas_id       = msg.get("meas_id")

        if mtype == "MOVE":
            print(
                f"[{CLIENT_ID}][exp {experiment_id}][meas {meas_id}] "
                f"MOVE received"
            )

            # Extract movement parameters from the server message
            overrides: Dict[str, Any] = {
                k: msg[k] for k in MOVE_PARAM_KEYS if k in msg
            }
            if overrides:
                print(f"[{CLIENT_ID}][exp {experiment_id}][meas {meas_id}] overrides: {overrides}")

            try:
                result = run_rover(base_config, overrides)

                response: Dict[str, Any] = {
                    "type":          "MOVE_DONE",
                    "experiment_id": experiment_id,
                    "cycle_id":      cycle_id,
                    "meas_id":       meas_id,
                    "id":            CLIENT_ID,
                    "status":        "ok",
                    "x":             result["x"],
                    "y":             result["y"],
                    "duration_s":    result["duration_s"],
                    "ts":            now_ms(),
                }
                print(
                    f"[{CLIENT_ID}][exp {experiment_id}][meas {meas_id}] "
                    f"MOVE_DONE  x={result['x']}  y={result['y']}  "
                    f"took={result['duration_s']}s"
                )

            except Exception as exc:
                # Report the error back to the server but keep running
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

    print(f"[{CLIENT_ID}] Shutting down.")
    sock.close()
    ctx.term()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Rover movement ZMQ client")
    p.add_argument(
        "--connect", default="tcp://127.0.0.1:5555",
        help="ZMQ endpoint of the orchestrator server"
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    rover_client(args.connect)


if __name__ == "__main__":
    main()