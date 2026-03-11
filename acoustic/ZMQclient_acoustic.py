#!/usr/bin/env python3
"""
zmqclient_acoustic.py

ZMQ DEALER client that performs real acoustic measurements.
Designed to work with zmq_orchestrator.py (ROUTER server).

Roles: meas1 or meas2.

Protocol:
  server -> START_MEAS  {experiment_id, cycle_id, meas_id, <optional meas params>}
  client -> MEAS_DONE   {experiment_id, cycle_id, meas_id, status, csv_file, n_mics, duration_s}
          | MEAS_DONE   {experiment_id, cycle_id, meas_id, status="error", error=<str>}

Optional measurement parameters forwarded from the server in START_MEAS:
  speaker_coordinates  : [x, y, z]  (list of floats)
  chirp_f_start        : float  [Hz]
  chirp_f_stop         : float  [Hz]
  chirp_duration       : float  [s]
  chirp_DC             : float
  chirp_ampl           : float  [0–1]

Usage:
  python zmqclient_acoustic.py --connect tcp://127.0.0.1:5555 --id meas1
  python zmqclient_acoustic.py --connect tcp://127.0.0.1:5555 --id meas2
"""

from __future__ import annotations

import argparse
import json
import signal
import time
from typing import Any, Dict, Optional
import zmq

from acousticMeasurement import load_config, read_system, run_acoustic_measurement

MEAS_PARAM_KEYS = [
    "speaker_coordinates",
    "chirp_f_start",
    "chirp_f_stop",
    "chirp_duration",
    "chirp_DC",
    "chirp_ampl",
]

def now_ms() -> int:
    return int(time.time() * 1000)

def jdump(obj: Dict[str, Any]) -> bytes:
    return json.dumps(obj, separators=(",", ":"), ensure_ascii=False).encode("utf-8")

def jload(b: bytes) -> Dict[str, Any]:
    return json.loads(b.decode("utf-8"))


def measurer_client(connect: str, client_id: str) -> None:
    if client_id not in {"meas1", "meas2"}:
        raise ValueError("--id must be 'meas1' or 'meas2'")

    base_config = load_config()
    if base_config.get("get_system_info"):
        read_system()

    ctx = zmq.Context.instance()
    sock = ctx.socket(zmq.DEALER)
    sock.linger = 0
    sock.setsockopt(zmq.IDENTITY, client_id.encode("utf-8"))
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

    send({"type": "HELLO", "id": client_id, "ts": now_ms()})
    print(f"[{client_id}] Connected to {connect}. Waiting for commands…")

    while not stop["flag"]:
        msg = recv(timeout_ms=1000)
        if msg is None:
            continue

        mtype         = msg.get("type")
        experiment_id = msg.get("experiment_id")
        cycle_id      = msg.get("cycle_id")
        meas_id       = msg.get("meas_id")

        if mtype == "START_MEAS":
            print(
                f"[{client_id}][exp {experiment_id}][meas {meas_id}] "
                f"START_MEAS received"
            )

            # Extract parameters from the server to override
            overrides: Dict[str, Any] = {
                k: msg[k] for k in MEAS_PARAM_KEYS if k in msg
            }
            if overrides:
                print(f"[{client_id}][exp {experiment_id}][meas {meas_id}] overrides: {overrides}")

            try:
                result = run_acoustic_measurement(base_config, overrides)

                response: Dict[str, Any] = {
                    "type":          "MEAS_DONE",
                    "experiment_id": experiment_id,
                    "cycle_id":      cycle_id,
                    "meas_id":       meas_id,
                    "id":            client_id,
                    "status":        "ok",
                    "csv_file":      result["csv_file"],
                    "n_mics":        result["n_mics"],
                    "duration_s":    result["duration_s"],
                    "ts":            now_ms(),
                }
                print(
                    f"[{client_id}][exp {experiment_id}][meas {meas_id}] "
                    f"MEAS_DONE  mics={result['n_mics']}  "
                    f"took={result['duration_s']}s  file={result['csv_file']}"
                )

            except Exception as exc:
                # Report the error back to the server but keep running
                response = {
                    "type":          "MEAS_DONE",
                    "experiment_id": experiment_id,
                    "cycle_id":      cycle_id,
                    "meas_id":       meas_id,
                    "id":            client_id,
                    "status":        "error",
                    "error":         str(exc),
                    "ts":            now_ms(),
                }
                print(f"[{client_id}][exp {experiment_id}][meas {meas_id}] ERROR: {exc}")

            send(response)

        elif mtype == "PING":
            send({"type": "PONG", "id": client_id, "ts": now_ms()})

        else:
            print(f"[{client_id}] unexpected message type '{mtype}' — sending ERROR")
            send({
                "type":          "ERROR",
                "experiment_id": experiment_id,
                "cycle_id":      cycle_id,
                "meas_id":       meas_id,
                "id":            client_id,
                "error":         f"Unexpected message type: {mtype}",
                "ts":            now_ms(),
            })

    print(f"[{client_id}] Shutting down.")
    sock.close()
    ctx.term()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Acoustic measurement ZMQ client")
    p.add_argument(
        "--connect", default="tcp://127.0.0.1:5555",
        help="ZMQ endpoint of the orchestrator server"
    )
    p.add_argument(
        "--id", required=True, choices=["meas1", "meas2"],
        help="Client identity (must be unique per machine)"
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    measurer_client(args.connect, args.id)


if __name__ == "__main__":
    main()