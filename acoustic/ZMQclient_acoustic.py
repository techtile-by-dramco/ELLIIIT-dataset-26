#!/usr/bin/env python3
"""
zmqclient_acoustic.py

Role: acoustic

Protocol:
  server -> START_MEAS  {experiment_id, cycle_id, meas_id, ts}
  client -> MEAS_DONE   {experiment_id, cycle_id, meas_id, status="ok"}
          | MEAS_DONE   {experiment_id, cycle_id, meas_id, status="error", error=<str>}

Usage:
  python zmqclient_acoustic.py --connect tcp://127.0.0.1:5555 --id acoustic
"""

from __future__ import annotations

import argparse
import json
import signal
import time
from typing import Any, Dict, Optional

import zmq

from acousticMeasurement import run_acoustic_measurement


def now_ms() -> int:
    return int(time.time() * 1000)


def jdump(obj: Dict[str, Any]) -> bytes:
    return json.dumps(obj, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def jload(b: bytes) -> Dict[str, Any]:
    return json.loads(b.decode("utf-8"))


def measurer_client(connect: str, client_id: str) -> None:
    if client_id not in {"acoustic"}:
        raise ValueError("--id must be 'acoustic'")

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
    print(f"[{client_id}] connected to {connect}. Waiting for commands…")

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

            try:
                # Config is loaded internally from the local config.json
                run_acoustic_measurement()

                response: Dict[str, Any] = {
                    "type":          "MEAS_DONE",
                    "experiment_id": experiment_id,
                    "cycle_id":      cycle_id,
                    "meas_id":       meas_id,
                    "id":            client_id,
                    "status":        "ok",
                    "ts":            now_ms(),
                }
                print(
                    f"[{client_id}][exp {experiment_id}][meas {meas_id}] MEAS_DONE"
                )

            except Exception as exc:
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

    print(f"[{client_id}] shutting down.")
    sock.close()
    ctx.term()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Acoustic measurement ZMQ client")
    p.add_argument(
        "--connect", default="tcp://127.0.0.1:5555",
        help="ZMQ endpoint of the orchestrator server",
    )
    p.add_argument(
        "--id", required=True, choices=["acoustic"],
        help="Client identity (must match the server's expected client name)",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    measurer_client(args.connect, args.id)


if __name__ == "__main__":
    main()