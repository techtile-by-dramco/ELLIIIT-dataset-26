#!/usr/bin/env python3
"""
zmq_orchestrator.py

ZMQ orchestrator (ROUTER server).
The control loop is configured via server_config.json. Optional rover position
logging is loaded from experiment-settings.yaml.

Clients are self-configured on their own hosts. The outer control messages carry
only coordination identifiers; no measurement parameters are forwarded.

Cycle:
  server   -> MOVE       (rover)    {experiment_id, cycle_id, meas_id, ts}
  rover    -> MOVE_DONE             {experiment_id, cycle_id, meas_id}
  server   -> capture position sample and append exp-<id>-positions.csv
  server   -> START_MEAS (acoustic) {experiment_id, cycle_id, meas_id, ts}
  acoustic -> MEAS_DONE             {experiment_id, cycle_id, meas_id}
  server   -> START_MEAS (rf)       {experiment_id, cycle_id, meas_id, ts}
  rf       -> MEAS_DONE             {experiment_id, cycle_id, meas_id}
  repeat

Run 4 terminals:

1) Server:
   python zmq_orchestrator.py server
   python zmq_orchestrator.py server --config path/to/server_config.json

2) Rover client:
   python zmqclient_rover.py --connect tcp://127.0.0.1:5555

3) Acoustic client:
   python zmqclient_acoustic.py --connect tcp://127.0.0.1:5555 --id acoustic

4) RF orchestrator client:
   python RF-orchestrator.py --connect tcp://127.0.0.1:5555 --id rf --experiment-settings ../experiment-settings.yaml

server_config.json layout:
{
    "experiment_id": "EXP001",
    "bind":          "tcp://*:5555",
    "cycles":        0,            // 0 = run forever
    "meas_start":    1,
    "timeouts": {
        "mov_s":   30.0,
        "meas_s":  60.0,
        "poll_ms": 250
    }
}
"""

from __future__ import annotations

import argparse
import csv
import json
import signal
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Set, TextIO, Tuple

import yaml
import zmq


def now_ms() -> int:
    return int(time.time() * 1000)


def jdump(obj: Dict[str, Any]) -> bytes:
    return json.dumps(obj, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def jload(b: bytes) -> Dict[str, Any]:
    return json.loads(b.decode("utf-8"))


DEFAULT_CONFIG_PATH = Path(__file__).parent / "server_config.json"
DEFAULT_EXPERIMENT_SETTINGS_PATH = Path(__file__).resolve().parents[1] / "experiment-settings.yaml"
POSITION_LOG_DIR = Path(__file__).resolve().parent / "record" / "data"
POSITION_LOG_FIELDS = [
    "experiment_id",
    "cycle_id",
    "meas_id",
    "move_status",
    "position_status",
    "captured_at_utc",
    "position_t",
    "x",
    "y",
    "z",
    "rotation_matrix_json",
    "error",
]


def load_server_config(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Server config not found: {path}")
    with open(path) as f:
        cfg = json.load(f)
    _validate_server_config(cfg)
    return cfg


def _validate_server_config(cfg: Dict[str, Any]) -> None:
    if "experiment_id" not in cfg:
        raise ValueError("server_config must contain 'experiment_id'")


@dataclass
class Timeouts:
    mov_s:   float = 30.0
    meas_s:  float = 60.0
    poll_ms: int   = 250

    @classmethod
    def from_config(cls, cfg: Dict[str, Any]) -> "Timeouts":
        t = cfg.get("timeouts", {})
        return cls(
            mov_s   = float(t.get("mov_s",   30.0)),
            meas_s  = float(t.get("meas_s",  60.0)),
            poll_ms = int(  t.get("poll_ms", 250)),
        )


@dataclass
class PositioningRuntime:
    enabled: bool
    positioner: Any = None
    log_file: Optional[TextIO] = None
    log_writer: Optional[csv.DictWriter] = None
    log_path: Optional[Path] = None


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_experiment_settings(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Experiment settings not found: {path}")
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def init_positioning_runtime(
    experiment_settings_path: Path,
    experiment_id: str,
) -> PositioningRuntime:
    settings = load_experiment_settings(experiment_settings_path)
    positioning = settings.get("positioning")
    if not isinstance(positioning, dict):
        raise ValueError(
            "Missing 'positioning' block in experiment settings. "
            "Position logging is required by the orchestrator."
        )

    try:
        from Positioner import PositionerClient
    except ImportError as exc:
        raise RuntimeError(
            "Positioning is enabled, but PositionerClient is unavailable. "
            "Install the 'positioner' package used by server/record/show_positions.py."
        ) from exc

    backend = str(positioning.get("protocol", "zmq")).lower()
    positioner = PositionerClient(config=positioning, backend=backend)
    positioner.start()

    POSITION_LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_path = POSITION_LOG_DIR / f"exp-{experiment_id}-positions.csv"
    log_file = open(log_path, "w", encoding="utf-8", newline="")
    log_writer = csv.DictWriter(log_file, fieldnames=POSITION_LOG_FIELDS)
    log_writer.writeheader()
    log_file.flush()

    print(
        "[server][positioning] started "
        f"(backend={backend}, wanted_body={positioning.get('wanted_body')})"
    )
    print(f"[server][positioning] logging to {log_path}")

    return PositioningRuntime(
        enabled=True,
        positioner=positioner,
        log_file=log_file,
        log_writer=log_writer,
        log_path=log_path,
    )


def close_positioning_runtime(runtime: PositioningRuntime) -> None:
    if runtime.positioner is not None:
        try:
            runtime.positioner.stop()
        except Exception as exc:
            print(f"[server][positioning] stop error: {exc}")

    if runtime.log_file is not None:
        runtime.log_file.close()


def capture_and_log_position(
    runtime: PositioningRuntime,
    *,
    experiment_id: str,
    cycle_id: int,
    meas_id: int,
    move_status: str,
) -> None:
    if not runtime.enabled or runtime.log_writer is None or runtime.log_file is None:
        return

    row: Dict[str, Any] = {
        "experiment_id": experiment_id,
        "cycle_id": cycle_id,
        "meas_id": meas_id,
        "move_status": move_status,
        "position_status": "no_data",
        "captured_at_utc": utc_now_iso(),
        "position_t": "",
        "x": "",
        "y": "",
        "z": "",
        "rotation_matrix_json": "",
        "error": "",
    }

    try:
        position = runtime.positioner.get_data()
        if position is None:
            row["error"] = "Positioner returned no data."
        else:
            row["position_status"] = "ok"
            row["position_t"] = getattr(position, "t", "")
            row["x"] = getattr(position, "x", "")
            row["y"] = getattr(position, "y", "")
            row["z"] = getattr(position, "z", "")
            row["rotation_matrix_json"] = json.dumps(
                getattr(position, "rotation_matrix", None),
                separators=(",", ":"),
                ensure_ascii=False,
            )
    except Exception as exc:
        row["position_status"] = "error"
        row["error"] = str(exc)

    runtime.log_writer.writerow(row)
    runtime.log_file.flush()

    print(
        f"[server][exp {experiment_id}][meas {meas_id}] "
        f"position capture status={row['position_status']} "
        f"x={row['x']} y={row['y']} z={row['z']}"
    )


def server_main(config_path: Path, experiment_settings_path: Path) -> None:
    cfg = load_server_config(config_path)
    print(f"[server] loaded config from {config_path}")

    experiment_id = cfg["experiment_id"]
    bind          = cfg.get("bind", "tcp://*:5555")
    cycles        = int(cfg.get("cycles", 0))
    meas_start    = int(cfg.get("meas_start", 1))
    timeouts      = Timeouts.from_config(cfg)

    ctx = zmq.Context.instance()
    sock = ctx.socket(zmq.ROUTER)
    sock.linger = 0
    sock.bind(bind)

    poller = zmq.Poller()
    poller.register(sock, zmq.POLLIN)

    alive: Set[str] = set()
    needed = {"rover", "acoustic", "rf"}
    stop   = {"flag": False}
    positioning_runtime = PositioningRuntime(enabled=False)

    def _sigint(_sig, _frame):
        stop["flag"] = True

    signal.signal(signal.SIGINT, _sigint)

    def send_to(client_id: str, msg: Dict[str, Any]) -> None:
        sock.send_multipart([client_id.encode("utf-8"), jdump(msg)])

    def recv_one(timeout_ms: int) -> Optional[Tuple[str, Dict[str, Any]]]:
        events = dict(poller.poll(timeout_ms))
        if sock not in events:
            return None
        parts = sock.recv_multipart()
        if len(parts) < 2:
            return None
        cid = parts[0].decode("utf-8", errors="replace")
        msg = jload(parts[-1])
        return cid, msg

    print(f"[server] bound at {bind}")
    print(f"[server] experiment_id={experiment_id}  cycles={'∞' if cycles == 0 else cycles}  meas_start={meas_start}")
    print("[server] waiting for HELLO from rover, acoustic, rf (up to 15 s)…")

    t0 = time.time()
    while not stop["flag"] and alive != needed and (time.time() - t0 < 15.0):
        got = recv_one(timeout_ms=timeouts.poll_ms)
        if got is None:
            continue
        cid, msg = got
        if msg.get("type") == "HELLO":
            alive.add(cid)
            print(f"[server] HELLO from '{cid}'  (alive={sorted(alive)})")
        else:
            print(f"[server] (pre-loop) got {msg.get('type')} from '{cid}'")

    if alive != needed:
        print(f"[server] WARNING: missing clients: {sorted(needed - alive)}")
        print("[server] continuing anyway; missing clients will cause timeouts.\n")

    cycle_id = 0
    meas_id  = meas_start - 1

    try:
        positioning_runtime = init_positioning_runtime(
            experiment_settings_path,
            experiment_id,
        )

        while not stop["flag"]:
            cycle_id += 1
            if cycles > 0 and cycle_id > cycles:
                break

            meas_id += 1

            # ------------------------------------------------------------------ MOVE
            move_msg: Dict[str, Any] = {
                "type":          "MOVE",
                "experiment_id": experiment_id,
                "cycle_id":      cycle_id,
                "meas_id":       meas_id,
                "ts":            now_ms(),
            }
            print(f"[server][exp {experiment_id}][meas {meas_id}] -> rover MOVE")
            send_to("rover", move_msg)

            got_move_done = False
            move_status = "unknown"
            deadline = time.time() + timeouts.mov_s
            while not stop["flag"] and time.time() < deadline and not got_move_done:
                got = recv_one(timeout_ms=timeouts.poll_ms)
                if got is None:
                    continue
                cid, msg = got
                mtype     = msg.get("type")
                mid_exp   = msg.get("experiment_id")
                mid_meas  = msg.get("meas_id")
                mid_cycle = msg.get("cycle_id")

                if (
                    cid == "rover"
                    and mtype == "MOVE_DONE"
                    and mid_exp   == experiment_id
                    and mid_meas  == meas_id
                    and mid_cycle == cycle_id
                ):
                    got_move_done = True
                    move_status = str(msg.get("status", "ok"))
                    print(f"[server][exp {experiment_id}][meas {meas_id}] <- rover MOVE_DONE  status={move_status}")
                    if move_status == "error":
                        print(f"[server][exp {experiment_id}][meas {meas_id}] rover error: {msg.get('error')}")
                elif mtype == "ERROR" and mid_exp == experiment_id and mid_meas == meas_id:
                    print(f"[server][exp {experiment_id}][meas {meas_id}] <- {cid} ERROR: {msg.get('error')}")
                elif mtype == "HELLO":
                    alive.add(cid)
                else:
                    print(
                        f"[server][exp {experiment_id}][meas {meas_id}] "
                        f"(ignored) <- {cid} {mtype} exp={mid_exp} meas={mid_meas}"
                    )

            if not got_move_done:
                print(f"[server][exp {experiment_id}][meas {meas_id}] TIMEOUT waiting MOVE_DONE — aborting")
                break

            capture_and_log_position(
                positioning_runtime,
                experiment_id=experiment_id,
                cycle_id=cycle_id,
                meas_id=meas_id,
                move_status=move_status,
            )

            # ------------------------------------------------------------ START_MEAS
            start_meas_msg: Dict[str, Any] = {
                "type":          "START_MEAS",
                "experiment_id": experiment_id,
                "cycle_id":      cycle_id,
                "meas_id":       meas_id,
                "ts":            now_ms(),
            }
            print(f"[server][exp {experiment_id}][meas {meas_id}] -> acoustic START_MEAS")
            send_to("acoustic", start_meas_msg)

            got_meas_done = False
            deadline = time.time() + timeouts.meas_s
            while not stop["flag"] and time.time() < deadline and not got_meas_done:
                got = recv_one(timeout_ms=timeouts.poll_ms)
                if got is None:
                    continue
                cid, msg = got
                mtype     = msg.get("type")
                mid_exp   = msg.get("experiment_id")
                mid_meas  = msg.get("meas_id")
                mid_cycle = msg.get("cycle_id")

                if (
                    cid == "acoustic"
                    and mtype == "MEAS_DONE"
                    and mid_exp   == experiment_id
                    and mid_meas  == meas_id
                    and mid_cycle == cycle_id
                ):
                    got_meas_done = True
                    status = msg.get("status", "ok")
                    print(f"[server][exp {experiment_id}][meas {meas_id}] <- acoustic MEAS_DONE  status={status}")
                    if status == "error":
                        print(f"[server][exp {experiment_id}][meas {meas_id}] acoustic error: {msg.get('error')}")
                elif mtype == "ERROR" and mid_exp == experiment_id and mid_meas == meas_id:
                    print(f"[server][exp {experiment_id}][meas {meas_id}] <- {cid} ERROR: {msg.get('error')}")
                elif mtype == "HELLO":
                    alive.add(cid)
                else:
                    print(
                        f"[server][exp {experiment_id}][meas {meas_id}] "
                        f"(ignored) <- {cid} {mtype} exp={mid_exp} meas={mid_meas}"
                    )

            if not got_meas_done:
                print(f"[server][exp {experiment_id}][meas {meas_id}] TIMEOUT waiting MEAS_DONE — aborting")
                break

            # -------------------------------------------------------------- RF MEAS
            start_rf_msg: Dict[str, Any] = {
                "type":          "START_MEAS",
                "experiment_id": experiment_id,
                "cycle_id":      cycle_id,
                "meas_id":       meas_id,
                "ts":            now_ms(),
            }
            print(f"[server][exp {experiment_id}][meas {meas_id}] -> rf START_MEAS")
            send_to("rf", start_rf_msg)

            got_rf_done = False
            deadline = time.time() + timeouts.meas_s
            while not stop["flag"] and time.time() < deadline and not got_rf_done:
                got = recv_one(timeout_ms=timeouts.poll_ms)
                if got is None:
                    continue
                cid, msg = got
                mtype     = msg.get("type")
                mid_exp   = msg.get("experiment_id")
                mid_meas  = msg.get("meas_id")
                mid_cycle = msg.get("cycle_id")

                if (
                    cid == "rf"
                    and mtype == "MEAS_DONE"
                    and mid_exp   == experiment_id
                    and mid_meas  == meas_id
                    and mid_cycle == cycle_id
                ):
                    got_rf_done = True
                    status = msg.get("status", "ok")
                    print(f"[server][exp {experiment_id}][meas {meas_id}] <- rf MEAS_DONE  status={status}")
                    if status == "error":
                        print(f"[server][exp {experiment_id}][meas {meas_id}] rf error: {msg.get('error')}")
                elif mtype == "ERROR" and mid_exp == experiment_id and mid_meas == meas_id:
                    print(f"[server][exp {experiment_id}][meas {meas_id}] <- {cid} ERROR: {msg.get('error')}")
                elif mtype == "HELLO":
                    alive.add(cid)
                else:
                    print(
                        f"[server][exp {experiment_id}][meas {meas_id}] "
                        f"(ignored) <- {cid} {mtype} exp={mid_exp} meas={mid_meas}"
                    )

            if not got_rf_done:
                print(f"[server][exp {experiment_id}][meas {meas_id}] TIMEOUT waiting rf MEAS_DONE — aborting")
                break

            print(f"[server][exp {experiment_id}][meas {meas_id}] cycle complete\n")

    finally:
        print("[server] shutting down")
        close_positioning_runtime(positioning_runtime)
        sock.close()
        ctx.term()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="ZMQ orchestrator with optional post-move position logging."
    )
    sub = p.add_subparsers(dest="mode", required=True)

    ps = sub.add_parser("server")
    ps.add_argument(
        "--config", default=str(DEFAULT_CONFIG_PATH),
        help="Path to server_config.json  (default: ./server_config.json)",
    )
    ps.add_argument(
        "--experiment-settings",
        default=str(DEFAULT_EXPERIMENT_SETTINGS_PATH),
        help="Path to experiment-settings.yaml used for optional position logging.",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    server_main(
        config_path=Path(args.config),
        experiment_settings_path=Path(args.experiment_settings),
    )


if __name__ == "__main__":
    main()
