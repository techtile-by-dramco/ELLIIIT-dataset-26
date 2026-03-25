#!/usr/bin/env python3
"""
ZMQ DEALER client for RF measurements.

Role: rf

Protocol with zmq_orchestrator.py:
  server -> START_MEAS  {experiment_id, cycle_id, meas_id, ts}
  client -> MEAS_DONE   {experiment_id, cycle_id, meas_id, status="ok"}
          | MEAS_DONE   {experiment_id, cycle_id, meas_id, status="error", error=<str>}

Per START_MEAS this client runs one RF sync cycle:
  1) Wait for ALIVE quorum from RF tiles (REP).
  2) Wait pre_sync_delay_s to let per-cycle SUB reconnects settle.
  3) Publish SYNC with "<cycle_id> <experiment_id>" (PUB).
  4) Wait for DONE quorum from RF tiles (REP).
"""

from __future__ import annotations

import argparse
import json
import signal
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional, TextIO

import yaml
import zmq


@dataclass(frozen=True)
class RFSyncConfig:
    num_subscribers: int
    host: str = "*"
    sync_port: str = "5557"
    alive_port: str = "5558"
    done_port: str = "5559"
    pre_sync_delay_s: float = 2.0
    wait_timeout_s: float = 600.0


@dataclass
class RFSyncRuntime:
    config: RFSyncConfig
    experiment_id: Optional[str]
    sync_socket: zmq.Socket
    alive_socket: zmq.Socket
    done_socket: zmq.Socket
    alive_poller: zmq.Poller
    done_poller: zmq.Poller
    log_file: Optional[TextIO]
    log_path: Optional[Path]


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


def load_rf_sync_config(settings_path: Path) -> RFSyncConfig:
    if not settings_path.exists():
        raise FileNotFoundError(f"Experiment settings not found: {settings_path}")

    with open(settings_path, "r", encoding="utf-8") as f:
        settings = yaml.safe_load(f) or {}

    rf_sync = settings.get("rf_sync")
    if not isinstance(rf_sync, dict):
        raise ValueError(
            "Missing 'rf_sync' block in experiment settings. "
            "Expected keys: num_subscribers, host, sync_port, alive_port, done_port, "
            "pre_sync_delay_s, wait_timeout_s."
        )

    if "num_subscribers" not in rf_sync:
        raise ValueError("Missing required key 'rf_sync.num_subscribers' in experiment settings.")

    cfg = RFSyncConfig(
        num_subscribers=int(rf_sync["num_subscribers"]),
        host=str(rf_sync.get("host", "*")),
        sync_port=str(rf_sync.get("sync_port", "5557")),
        alive_port=str(rf_sync.get("alive_port", "5558")),
        done_port=str(rf_sync.get("done_port", "5559")),
        pre_sync_delay_s=float(rf_sync.get("pre_sync_delay_s", 2.0)),
        wait_timeout_s=float(rf_sync.get("wait_timeout_s", 600.0)),
    )
    if cfg.num_subscribers <= 0:
        raise ValueError("rf_sync.num_subscribers must be > 0")
    if cfg.pre_sync_delay_s < 0:
        raise ValueError("rf_sync.pre_sync_delay_s must be >= 0")
    if cfg.wait_timeout_s <= 0:
        raise ValueError("rf_sync.wait_timeout_s must be > 0")

    return cfg


def init_rf_sync_runtime(ctx: zmq.Context, config: RFSyncConfig) -> RFSyncRuntime:
    sync_socket = ctx.socket(zmq.PUB)
    alive_socket = ctx.socket(zmq.REP)
    done_socket = ctx.socket(zmq.REP)

    sync_socket.linger = 0
    alive_socket.linger = 0
    done_socket.linger = 0

    sync_socket.bind(f"tcp://{config.host}:{config.sync_port}")
    alive_socket.bind(f"tcp://{config.host}:{config.alive_port}")
    done_socket.bind(f"tcp://{config.host}:{config.done_port}")

    alive_poller = zmq.Poller()
    alive_poller.register(alive_socket, zmq.POLLIN)

    done_poller = zmq.Poller()
    done_poller.register(done_socket, zmq.POLLIN)

    print(
        "[rf-sync] bound sockets "
        f"(sync={config.sync_port}, alive={config.alive_port}, done={config.done_port}) "
        f"pre_sync_delay={config.pre_sync_delay_s:.2f}s "
        f"for {config.num_subscribers} subscribers"
    )

    return RFSyncRuntime(
        config=config,
        experiment_id=None,
        sync_socket=sync_socket,
        alive_socket=alive_socket,
        done_socket=done_socket,
        alive_poller=alive_poller,
        done_poller=done_poller,
        log_file=None,
        log_path=None,
    )


def close_rf_sync_runtime(runtime: RFSyncRuntime) -> None:
    if runtime.log_file is not None:
        runtime.log_file.close()
    runtime.sync_socket.close()
    runtime.alive_socket.close()
    runtime.done_socket.close()


def _ensure_experiment_context(runtime: RFSyncRuntime, experiment_id: Any) -> str:
    exp_id = str(experiment_id) if experiment_id is not None else ""
    if not exp_id:
        raise RuntimeError("Missing experiment_id in START_MEAS.")

    if runtime.experiment_id is None:
        runtime.experiment_id = exp_id
        output_dir = Path(__file__).resolve().parent / "data"
        output_dir.mkdir(parents=True, exist_ok=True)
        runtime.log_path = output_dir / f"exp-{exp_id}.yml"
        runtime.log_file = open(runtime.log_path, "w", encoding="utf-8")
        runtime.log_file.write(f"experiment: {exp_id}\n")
        runtime.log_file.write(f"num_subscribers: {runtime.config.num_subscribers}\n")
        runtime.log_file.write("measurements:\n")
        runtime.log_file.flush()
        print(f"[rf-sync] logging to {runtime.log_path}")
    elif runtime.experiment_id != exp_id:
        raise RuntimeError(
            f"experiment_id changed from '{runtime.experiment_id}' to '{exp_id}' in one process."
        )

    return runtime.experiment_id


def _wait_for_quorum(
    socket: zmq.Socket,
    poller: zmq.Poller,
    expected: int,
    timeout_s: float,
    phase: str,
) -> list[str]:
    messages: list[str] = []
    deadline = time.monotonic() + timeout_s

    while len(messages) < expected:
        remaining_s = deadline - time.monotonic()
        if remaining_s <= 0:
            raise RuntimeError(
                f"{phase} timeout: received {len(messages)}/{expected} messages "
                f"within {timeout_s:.1f}s"
            )

        poll_ms = max(1, min(1000, int(remaining_s * 1000)))
        events = dict(poller.poll(poll_ms))
        if socket not in events:
            continue

        message = socket.recv_string()
        messages.append(message)
        print(f"[rf-sync][{phase}] {message} ({len(messages)}/{expected})")
        socket.send_string("ACK")

    return messages


def _append_measurement_log(
    runtime: RFSyncRuntime,
    experiment_id: Any,
    cycle_id: Any,
    meas_id: Any,
    active_tiles: list[str],
) -> None:
    if runtime.log_file is None:
        raise RuntimeError("RF sync log file is not initialized.")

    runtime.log_file.write(f"  - meas_id: {meas_id}\n")
    runtime.log_file.write(f"    cycle_id: {cycle_id}\n")
    runtime.log_file.write(f"    experiment_id: {experiment_id}\n")
    runtime.log_file.write("    active_tiles:\n")
    for tile in active_tiles:
        runtime.log_file.write(f"      - {tile}\n")
    runtime.log_file.flush()


def run_rf_measurement(
    *,
    experiment_id: Any,
    cycle_id: Any,
    meas_id: Any,
    runtime: RFSyncRuntime,
) -> Dict[str, Any]:
    expected = runtime.config.num_subscribers
    timeout_s = runtime.config.wait_timeout_s
    sync_experiment_id = _ensure_experiment_context(runtime, experiment_id)

    print(f"[rf-sync][exp {experiment_id}][meas {meas_id}] waiting for ALIVE quorum...")
    alive_messages = _wait_for_quorum(
        runtime.alive_socket,
        runtime.alive_poller,
        expected=expected,
        timeout_s=timeout_s,
        phase="ALIVE",
    )

    if runtime.config.pre_sync_delay_s > 0:
        print(
            f"[rf-sync][exp {experiment_id}][meas {meas_id}] "
            f"waiting {runtime.config.pre_sync_delay_s:.2f}s before SYNC"
        )
        time.sleep(runtime.config.pre_sync_delay_s)

    sync_payload = f"{cycle_id} {sync_experiment_id}"
    runtime.sync_socket.send_string(sync_payload)
    print(f"[rf-sync][exp {experiment_id}][meas {meas_id}] SYNC {sync_payload}")

    print(f"[rf-sync][exp {experiment_id}][meas {meas_id}] waiting for DONE quorum...")
    done_messages = _wait_for_quorum(
        runtime.done_socket,
        runtime.done_poller,
        expected=expected,
        timeout_s=timeout_s,
        phase="DONE",
    )

    active_tiles = sorted(set(alive_messages))
    _append_measurement_log(runtime, experiment_id, cycle_id, meas_id, active_tiles)

    return {
        "experiment_id": sync_experiment_id,
        "alive_count": len(alive_messages),
        "done_count": len(done_messages),
        "active_tiles": active_tiles,
    }


def rf_client(connect: str, client_id: str, settings_path: Path) -> None:
    if client_id not in {"rf"}:
        raise ValueError("--id must be 'rf'")

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

    config = load_rf_sync_config(settings_path)
    runtime = init_rf_sync_runtime(ctx, config)

    send({"type": "HELLO", "id": client_id, "ts": now_ms()})
    print(f"[{client_id}] connected to {connect}. Waiting for commands...")

    try:
        while not stop["flag"]:
            msg = recv(timeout_ms=1000)
            if msg is None:
                continue

            mtype = msg.get("type")
            experiment_id = msg.get("experiment_id")
            cycle_id = msg.get("cycle_id")
            meas_id = msg.get("meas_id")

            if mtype == "START_MEAS":
                print(
                    f"[{client_id}][exp {experiment_id}][meas {meas_id}] "
                    "START_MEAS received"
                )

                try:
                    summary = run_rf_measurement(
                        experiment_id=experiment_id,
                        cycle_id=cycle_id,
                        meas_id=meas_id,
                        runtime=runtime,
                    )

                    response: Dict[str, Any] = {
                        "type": "MEAS_DONE",
                        "experiment_id": experiment_id,
                        "cycle_id": cycle_id,
                        "meas_id": meas_id,
                        "id": client_id,
                        "status": "ok",
                        "ts": now_ms(),
                    }
                    print(
                        f"[{client_id}][exp {experiment_id}][meas {meas_id}] "
                        "MEAS_DONE "
                        f"(alive={summary['alive_count']}, done={summary['done_count']})"
                    )

                except Exception as exc:
                    response = {
                        "type": "MEAS_DONE",
                        "experiment_id": experiment_id,
                        "cycle_id": cycle_id,
                        "meas_id": meas_id,
                        "id": client_id,
                        "status": "error",
                        "error": str(exc),
                        "ts": now_ms(),
                    }
                    print(
                        f"[{client_id}][exp {experiment_id}][meas {meas_id}] "
                        f"ERROR: {exc}"
                    )

                send(response)

            elif mtype == "PING":
                send({"type": "PONG", "id": client_id, "ts": now_ms()})

            else:
                print(f"[{client_id}] unexpected message type '{mtype}' -- sending ERROR")
                send(
                    {
                        "type": "ERROR",
                        "experiment_id": experiment_id,
                        "cycle_id": cycle_id,
                        "meas_id": meas_id,
                        "id": client_id,
                        "error": f"Unexpected message type: {mtype}",
                        "ts": now_ms(),
                    }
                )
    finally:
        print(f"[{client_id}] shutting down.")
        close_rf_sync_runtime(runtime)
        sock.close()
        ctx.term()


def parse_args() -> argparse.Namespace:
    default_settings = Path(__file__).resolve().parents[2] / "experiment-settings.yaml"
    p = argparse.ArgumentParser(description="RF measurement ZMQ client")
    p.add_argument(
        "--connect",
        default=None,
        help="ZMQ endpoint of the orchestrator server; defaults to server.host from experiment-settings.yaml",
    )
    p.add_argument(
        "--id",
        required=True,
        choices=["rf"],
        help="Client identity (must match the server's expected client name)",
    )
    p.add_argument(
        "--experiment-settings",
        default=str(default_settings),
        help="Path to experiment-settings.yaml",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    settings_path = Path(args.experiment_settings)
    connect = resolve_orchestrator_connect(args.connect, settings_path)
    rf_client(connect, args.id, settings_path)


if __name__ == "__main__":
    main()
