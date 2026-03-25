#!/usr/bin/env python3
"""
zmq_orchestrator.py

ZMQ orchestrator (ROUTER server).
The control loop is configured via serverConfig.yaml. Optional rover position
logging is loaded from experiment-settings.yaml.

Clients are self-configured on their own hosts. The outer control messages carry
only coordination identifiers; no measurement parameters are forwarded.

Cycle:
  ref      -> HELLO                 {id="ref", service="run-ref", status="started"}
  server   -> PING       (ref)      {ping_id, ts}
  ref      -> PONG                  {ping_id, ts}
  server   -> MOVE       (rover)    {experiment_id, cycle_id, meas_id, ts}
  rover    -> MOVE_DONE             {experiment_id, cycle_id, meas_id}
  server   -> capture position sample and append exp-<id>-positions.csv
  server   -> START_MEAS (acoustic) {experiment_id, cycle_id, meas_id, ts}
  acoustic -> MEAS_DONE             {experiment_id, cycle_id, meas_id}
  server   -> START_MEAS (rf)       {experiment_id, cycle_id, meas_id, ts}
  rf       -> MEAS_DONE             {experiment_id, cycle_id, meas_id}
  repeat

Run 5 terminals:

1) Server:
   python zmq_orchestrator.py server
   python zmq_orchestrator.py server --config path/to/serverConfig.yaml

2) Rover client:
    python zmqclient_rover.py --connect tcp://127.0.0.1:5555 --config-file config.yaml

3) Acoustic client:
   python zmqclient_acoustic.py --connect tcp://127.0.0.1:5555 --id acoustic

4) RF orchestrator client:
   python RF-orchestrator.py --connect tcp://127.0.0.1:5555 --id rf --experiment-settings ../experiment-settings.yaml

5) Reference transmitter:
   python client/run-ref.py --config-file ../experiment-settings.yaml --freq 920e6 --rate 250e3 --duration 1E6 --channels 0 --wave-ampl 0.8 --gain 73 -w sine --wave-freq 0

serverConfig.yaml layout:
experiment_id: "EXP001"
bind: "tcp://*:5555"
cycles: 0          # 0 = run forever
meas_start: 1
timeouts:
  ref_s: 5.0
  mov_s: 30.0
  meas_s: 60.0
  poll_ms: 250
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import signal
import sys
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


LOGGER = logging.getLogger("zmq_orchestrator")
ANSI_RESET = "\033[0m"
ENTITY_COLORS = {
    "server": "\033[36m",
    "ref": "\033[35m",
    "rover": "\033[33m",
    "acoustic": "\033[32m",
    "rf": "\033[34m",
    "positioning": "\033[96m",
}
USE_COLOR = sys.stderr.isatty()


def setup_logging() -> None:
    if LOGGER.handlers:
        return

    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter(
            "[%(asctime)s] [%(levelname)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    LOGGER.addHandler(handler)
    LOGGER.setLevel(logging.INFO)
    LOGGER.propagate = False


def _fmt_log_value(value: Any) -> str:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, set):
        value = sorted(value)
    if isinstance(value, (list, tuple)):
        return "[" + ",".join(_fmt_log_value(item) for item in value) + "]"
    if isinstance(value, dict):
        return json.dumps(value, separators=(",", ":"), ensure_ascii=False)
    if isinstance(value, str):
        if not value:
            return '""'
        if any(ch.isspace() for ch in value):
            return json.dumps(value, ensure_ascii=False)
        return _colorize_entity(value)
    return str(value)


def _colorize_entity(value: str) -> str:
    if not USE_COLOR:
        return value
    color = ENTITY_COLORS.get(value)
    if color is None:
        return value
    return f"{color}{value}{ANSI_RESET}"


def log_event(level: int, event: str, **fields: Any) -> None:
    payload = " ".join(
        f"{key}={_fmt_log_value(value)}"
        for key, value in fields.items()
        if value is not None
    )
    message = f"[{event}]"
    if payload:
        message = f"{message} {payload}"
    LOGGER.log(level, message)


def log_banner(title: str, **fields: Any) -> None:
    payload = " ".join(
        f"{key}={_fmt_log_value(value)}"
        for key, value in fields.items()
        if value is not None
    )
    suffix = f" {payload}" if payload else ""
    LOGGER.info("========== %s%s ==========", title, suffix)


def summarize_message(msg: Dict[str, Any]) -> Dict[str, Any]:
    summary: Dict[str, Any] = {}
    field_map = {
        "type": "msg_type",
        "status": "status",
        "id": "client_id",
        "service": "service",
        "ping_id": "ping_id",
        "experiment_id": "experiment_id",
        "cycle_id": "cycle_id",
        "meas_id": "meas_id",
        "ts": "ts",
        "error": "error",
    }
    for src_key, dst_key in field_map.items():
        if src_key in msg:
            summary[dst_key] = msg.get(src_key)
    return summary


def log_send(target: str, msg: Dict[str, Any], **fields: Any) -> None:
    log_event(logging.INFO, "send", target=target, **summarize_message(msg), **fields)


def log_recv(source: str, msg: Dict[str, Any], **fields: Any) -> None:
    log_event(logging.INFO, "recv", source=source, **summarize_message(msg), **fields)


DEFAULT_CONFIG_PATH = Path(__file__).parent / "serverConfig.yaml"
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
    with open(path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}
    _validate_server_config(cfg)
    return cfg


def _validate_server_config(cfg: Dict[str, Any]) -> None:
    if "experiment_id" not in cfg:
        raise ValueError("server_config must contain 'experiment_id'")


@dataclass
class Timeouts:
    ref_s:   float = 5.0
    mov_s:   float = 30.0
    meas_s:  float = 60.0
    poll_ms: int   = 250

    @classmethod
    def from_config(cls, cfg: Dict[str, Any]) -> "Timeouts":
        t = cfg.get("timeouts", {})
        return cls(
            ref_s   = float(t.get("ref_s",   5.0)),
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

    log_event(
        logging.INFO,
        "positioning.start",
        backend=backend,
        wanted_body=positioning.get("wanted_body"),
        log_path=log_path,
    )

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
            log_event(logging.WARNING, "positioning.stop_error", error=str(exc))

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

    log_event(
        logging.INFO,
        "positioning.capture",
        experiment_id=experiment_id,
        cycle_id=cycle_id,
        meas_id=meas_id,
        move_status=move_status,
        position_status=row["position_status"],
        x=row["x"],
        y=row["y"],
        z=row["z"],
        error=row["error"] or None,
    )


def server_main(config_path: Path, experiment_settings_path: Path) -> None:
    setup_logging()
    cfg = load_server_config(config_path)
    log_event(logging.INFO, "config.loaded", config_path=config_path)

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
    startup_needed = {"rover", "acoustic", "rf"}
    measurement_needed = {"ref"}
    stop   = {"flag": False}
    positioning_runtime = PositioningRuntime(enabled=False)

    def _sigint(_sig, _frame):
        stop["flag"] = True

    signal.signal(signal.SIGINT, _sigint)

    def send_to(client_id: str, msg: Dict[str, Any]) -> None:
        sock.send_multipart([client_id.encode("utf-8"), jdump(msg)])
        log_send(client_id, msg)

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

    def ensure_client_active(client_id: str, *, timeout_s: float, label: str) -> None:
        deadline = time.time() + timeout_s

        if client_id not in alive:
            log_event(
                logging.INFO,
                "healthcheck.wait_for_hello",
                client=client_id,
                label=label,
                timeout_s=timeout_s,
            )
            while not stop["flag"] and time.time() < deadline and client_id not in alive:
                got = recv_one(timeout_ms=timeouts.poll_ms)
                if got is None:
                    continue

                cid, msg = got
                mtype = msg.get("type")

                if mtype == "HELLO":
                    alive.add(cid)
                    log_recv(cid, msg, alive=sorted(alive))
                    continue

                if cid == client_id and mtype == "ERROR":
                    raise RuntimeError(
                        f"Mandatory client '{client_id}' reported an error during {label}: "
                        f"{msg.get('error')}"
                    )

                log_event(
                    logging.WARNING,
                    "recv.ignored",
                    phase=label,
                    source=cid,
                    msg_type=mtype,
                )

            if client_id not in alive:
                raise RuntimeError(
                    f"Mandatory client '{client_id}' did not announce readiness before {label}. "
                    "Ensure client/run-ref.py is transmitting and connected to the orchestrator."
                )

        ping_id = f"{client_id}-{now_ms()}"
        send_to(
            client_id,
            {
                "type": "PING",
                "id": "server",
                "ping_id": ping_id,
                "ts": now_ms(),
            },
        )
        log_event(
            logging.INFO,
            "healthcheck.start",
            client=client_id,
            label=label,
            timeout_s=timeout_s,
            ping_id=ping_id,
        )

        while not stop["flag"] and time.time() < deadline:
            got = recv_one(timeout_ms=timeouts.poll_ms)
            if got is None:
                continue

            cid, msg = got
            mtype = msg.get("type")
            mid_ping = msg.get("ping_id")

            if cid == client_id and mtype == "PONG" and mid_ping == ping_id:
                log_recv(cid, msg, label=label, matched=True)
                return

            if cid == client_id and mtype == "ERROR":
                raise RuntimeError(
                    f"Mandatory client '{client_id}' reported an error during {label}: "
                    f"{msg.get('error')}"
                )

            if mtype == "HELLO":
                alive.add(cid)
                log_recv(cid, msg, alive=sorted(alive))
            else:
                log_event(
                    logging.WARNING,
                    "recv.ignored",
                    phase=label,
                    source=cid,
                    msg_type=mtype,
                    ping_id=mid_ping,
                )

        raise RuntimeError(
            f"Timeout waiting for mandatory client '{client_id}' during {label}."
        )

    try:
        log_banner(
            "server.start",
            bind=bind,
            experiment_id=experiment_id,
            cycles="∞" if cycles == 0 else cycles,
            meas_start=meas_start,
        )
        log_event(
            logging.INFO,
            "handshake.wait",
            expected_clients=sorted(startup_needed),
            deferred_clients=sorted(measurement_needed),
            timeout_s=15.0,
        )

        t0 = time.time()
        while not stop["flag"] and not startup_needed.issubset(alive) and (time.time() - t0 < 15.0):
            got = recv_one(timeout_ms=timeouts.poll_ms)
            if got is None:
                continue
            cid, msg = got
            if msg.get("type") == "HELLO":
                alive.add(cid)
                log_recv(cid, msg, alive=sorted(alive))
            else:
                log_event(
                    logging.WARNING,
                    "handshake.unexpected",
                    source=cid,
                    msg_type=msg.get("type"),
                )

        if not startup_needed.issubset(alive):
            log_event(
                logging.WARNING,
                "handshake.partial",
                connected=sorted(alive),
                missing=sorted(startup_needed - alive),
            )
        else:
            log_event(
                logging.INFO,
                "handshake.complete",
                connected=sorted(alive),
                pending_measurement_clients=sorted(measurement_needed - alive),
            )
    except Exception:
        sock.close()
        ctx.term()
        raise

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
            log_banner(
                "cycle.start",
                experiment_id=experiment_id,
                cycle_id=cycle_id,
                meas_id=meas_id,
            )

            ensure_client_active(
                "ref",
                timeout_s=timeouts.ref_s,
                label=f"pre-cycle meas {meas_id}",
            )

            # ------------------------------------------------------------------ MOVE
            move_msg: Dict[str, Any] = {
                "type":          "MOVE",
                "experiment_id": experiment_id,
                "cycle_id":      cycle_id,
                "meas_id":       meas_id,
                "ts":            now_ms(),
            }
            send_to("rover", move_msg)
            log_event(
                logging.INFO,
                "phase.start",
                phase="move",
                experiment_id=experiment_id,
                cycle_id=cycle_id,
                meas_id=meas_id,
                target="rover",
                timeout_s=timeouts.mov_s,
            )

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
                    log_recv(cid, msg, matched=True, phase="move")
                    if move_status == "error":
                        log_event(
                            logging.ERROR,
                            "phase.error",
                            phase="move",
                            experiment_id=experiment_id,
                            cycle_id=cycle_id,
                            meas_id=meas_id,
                            source=cid,
                            error=msg.get("error"),
                        )
                elif mtype == "ERROR" and mid_exp == experiment_id and mid_meas == meas_id:
                    log_recv(cid, msg, matched=False, phase="move")
                elif mtype == "HELLO":
                    alive.add(cid)
                    log_recv(cid, msg, alive=sorted(alive))
                else:
                    log_event(
                        logging.WARNING,
                        "recv.ignored",
                        phase="move",
                        source=cid,
                        msg_type=mtype,
                        experiment_id=mid_exp,
                        cycle_id=mid_cycle,
                        meas_id=mid_meas,
                    )

            if not got_move_done:
                log_event(
                    logging.ERROR,
                    "phase.timeout",
                    phase="move",
                    experiment_id=experiment_id,
                    cycle_id=cycle_id,
                    meas_id=meas_id,
                    expected="MOVE_DONE",
                    timeout_s=timeouts.mov_s,
                )
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
            send_to("acoustic", start_meas_msg)
            log_event(
                logging.INFO,
                "phase.start",
                phase="acoustic",
                experiment_id=experiment_id,
                cycle_id=cycle_id,
                meas_id=meas_id,
                target="acoustic",
                timeout_s=timeouts.meas_s,
            )

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
                    log_recv(cid, msg, matched=True, phase="acoustic")
                    if status == "error":
                        log_event(
                            logging.ERROR,
                            "phase.error",
                            phase="acoustic",
                            experiment_id=experiment_id,
                            cycle_id=cycle_id,
                            meas_id=meas_id,
                            source=cid,
                            error=msg.get("error"),
                        )
                elif mtype == "ERROR" and mid_exp == experiment_id and mid_meas == meas_id:
                    log_recv(cid, msg, matched=False, phase="acoustic")
                elif mtype == "HELLO":
                    alive.add(cid)
                    log_recv(cid, msg, alive=sorted(alive))
                else:
                    log_event(
                        logging.WARNING,
                        "recv.ignored",
                        phase="acoustic",
                        source=cid,
                        msg_type=mtype,
                        experiment_id=mid_exp,
                        cycle_id=mid_cycle,
                        meas_id=mid_meas,
                    )

            if not got_meas_done:
                log_event(
                    logging.ERROR,
                    "phase.timeout",
                    phase="acoustic",
                    experiment_id=experiment_id,
                    cycle_id=cycle_id,
                    meas_id=meas_id,
                    expected="MEAS_DONE",
                    timeout_s=timeouts.meas_s,
                )
                break

            # -------------------------------------------------------------- RF MEAS
            start_rf_msg: Dict[str, Any] = {
                "type":          "START_MEAS",
                "experiment_id": experiment_id,
                "cycle_id":      cycle_id,
                "meas_id":       meas_id,
                "ts":            now_ms(),
            }
            send_to("rf", start_rf_msg)
            log_event(
                logging.INFO,
                "phase.start",
                phase="rf",
                experiment_id=experiment_id,
                cycle_id=cycle_id,
                meas_id=meas_id,
                target="rf",
                timeout_s=timeouts.meas_s,
            )

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
                    log_recv(cid, msg, matched=True, phase="rf")
                    if status == "error":
                        log_event(
                            logging.ERROR,
                            "phase.error",
                            phase="rf",
                            experiment_id=experiment_id,
                            cycle_id=cycle_id,
                            meas_id=meas_id,
                            source=cid,
                            error=msg.get("error"),
                        )
                elif mtype == "ERROR" and mid_exp == experiment_id and mid_meas == meas_id:
                    log_recv(cid, msg, matched=False, phase="rf")
                elif mtype == "HELLO":
                    alive.add(cid)
                    log_recv(cid, msg, alive=sorted(alive))
                else:
                    log_event(
                        logging.WARNING,
                        "recv.ignored",
                        phase="rf",
                        source=cid,
                        msg_type=mtype,
                        experiment_id=mid_exp,
                        cycle_id=mid_cycle,
                        meas_id=mid_meas,
                    )

            if not got_rf_done:
                log_event(
                    logging.ERROR,
                    "phase.timeout",
                    phase="rf",
                    experiment_id=experiment_id,
                    cycle_id=cycle_id,
                    meas_id=meas_id,
                    expected="MEAS_DONE",
                    timeout_s=timeouts.meas_s,
                )
                break

            log_banner(
                "cycle.complete",
                experiment_id=experiment_id,
                cycle_id=cycle_id,
                meas_id=meas_id,
            )

    finally:
        log_banner("server.shutdown", experiment_id=experiment_id)
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
        help="Path to serverConfig.yaml  (default: ./serverConfig.yaml)",
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
