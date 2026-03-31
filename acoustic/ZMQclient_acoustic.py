#!/usr/bin/env python3
"""
zmqclient_acoustic.py

Role: acoustic

Protocol:
  server -> START_MEAS  {experiment_id, cycle_id, meas_id, ts}
  client -> MEAS_DONE   {experiment_id, cycle_id, meas_id, status="ok"}
          | MEAS_DONE   {experiment_id, cycle_id, meas_id, status="error", error=<str>}

Usage:
  python zmqclient_acoustic.py --id acoustic
  python zmqclient_acoustic.py --connect tcp://SERVER:5555 --id acoustic
  python zmqclient_acoustic.py --connect tcp://SERVER:5555 --id acoustic --log-file
"""

from __future__ import annotations

import argparse
import json
import logging
import logging.handlers
import signal
import time
import traceback
from pathlib import Path
from typing import Any, Dict, Optional

import yaml
import zmq

from acousticMeasurement import run_acoustic_measurement

DEFAULT_EXPERIMENT_SETTINGS_PATH = Path(__file__).resolve().parents[1] / "experiment-settings.yaml"
DEFAULT_LOG_PATH = Path(__file__).resolve().parent / "logs" / "zmqclient_acoustic.log"

def setup_logger(log_path: Path) -> logging.Logger:
    """
    Returns a logger that writes to both the console and a rotating log file.
    Each run appends to the same file; old content is kept up to 5 × 5 MB.
    """
    log_path.parent.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("acoustic_client")
    logger.setLevel(logging.DEBUG)

    fmt = logging.Formatter(
        fmt="%(asctime)s.%(msecs)03d  %(levelname)-8s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    fh = logging.handlers.RotatingFileHandler(
        log_path, maxBytes=5 * 1024 * 1024, backupCount=5, encoding="utf-8"
    )
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)

    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)

    logger.addHandler(fh)
    logger.addHandler(ch)

    return logger


log = logging.getLogger("acoustic_client")

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
        log.debug("SEND %s", msg)

    def recv(timeout_ms: int = 1000) -> Optional[Dict[str, Any]]:
        events = dict(poller.poll(timeout_ms))
        if sock not in events:
            return None
        msg = jload(sock.recv())
        log.debug("RECV %s", msg)
        return msg

    log.info("Connecting to %s as '%s'", connect, client_id)
    send({"type": "HELLO", "id": client_id, "ts": now_ms()})
    print(f"[{client_id}] connected to {connect}. Waiting for commands…")
    log.info("[%s] connected to %s. Waiting for commands…", client_id, connect)

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
            log.info(
                "[%s][exp %s][cycle %s][meas %s] START_MEAS received",
                client_id, experiment_id, cycle_id, meas_id,
            )

            try:
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
                log.info(
                    "[%s][exp %s][cycle %s][meas %s] MEAS_DONE status=ok",
                    client_id, experiment_id, cycle_id, meas_id,
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
                log.error(
                    "[%s][exp %s][cycle %s][meas %s] measurement error: %s",
                    client_id, experiment_id, cycle_id, meas_id, exc,
                )
                log.debug("Traceback:\n%s", traceback.format_exc())

            send(response)

        elif mtype == "PING":
            log.debug("[%s] PING received — sending PONG", client_id)
            send({"type": "PONG", "id": client_id, "ts": now_ms()})

        else:
            print(f"[{client_id}] unexpected message type '{mtype}' — sending ERROR")
            log.warning(
                "[%s][exp %s][cycle %s][meas %s] unexpected message type '%s'",
                client_id, experiment_id, cycle_id, meas_id, mtype,
            )
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
    log.info("[%s] shutting down (SIGINT received).", client_id)
    sock.close()
    ctx.term()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Acoustic measurement ZMQ client")
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
        "--id", required=True, choices=["acoustic"],
        help="Client identity (must match the server's expected client name)",
    )
    p.add_argument(
        "--log-file",
        default=str(DEFAULT_LOG_PATH),
        help="Path to the rotating log file (default: logs/zmqclient_acoustic.log next to this script)",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()

    global log
    log = setup_logger(Path(args.log_file))
    log.info("=" * 60)
    log.info("zmqclient_acoustic starting up")
    log.info("Log file: %s", args.log_file)

    try:
        connect = resolve_orchestrator_connect(
            args.connect,
            Path(args.experiment_settings),
        )
        measurer_client(connect, args.id)
    except Exception:
        log.critical("Unhandled exception — process is crashing:\n%s", traceback.format_exc())
        raise


if __name__ == "__main__":
    main()