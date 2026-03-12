#!/usr/bin/env python3
"""
zmq_orchestrator.py

ZMQ orchestrator (ROUTER server).
All configuration is read from server_config.json — which contains only
server-side concerns (experiment identity, cycle count, timeouts, bind address).

Clients (rover, acoustic) are fully self-configured via their own local configs.
Messages carry only coordination identifiers; no parameters are forwarded.

Cycle:
  server   -> MOVE       (rover)    {experiment_id, cycle_id, meas_id, ts}
  rover    -> MOVE_DONE             {experiment_id, cycle_id, meas_id}
  server   -> START_MEAS (acoustic) {experiment_id, cycle_id, meas_id, ts}
  acoustic -> MEAS_DONE             {experiment_id, cycle_id, meas_id}
  repeat

Run 3 terminals:

1) Server:
   python zmq_orchestrator.py server
   python zmq_orchestrator.py server --config path/to/server_config.json

2) Rover client:
   python zmqclient_rover.py --connect tcp://127.0.0.1:5555

3) Acoustic client:
   python zmqclient_acoustic.py --connect tcp://127.0.0.1:5555 --id acoustic

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
import json
import signal
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional, Set, Tuple

import zmq


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

DEFAULT_CONFIG_PATH = Path(__file__).parent / "server_config.json"


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


# ---------------------------------------------------------------------------
# server
# ---------------------------------------------------------------------------

def server_main(config_path: Path) -> None:
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
    needed = {"rover", "acoustic"}
    stop   = {"flag": False}

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
    print("[server] waiting for HELLO from rover, acoustic (up to 15 s)…")

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
                status = msg.get("status", "ok")
                print(f"[server][exp {experiment_id}][meas {meas_id}] <- rover MOVE_DONE  status={status}")
                if status == "error":
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

        print(f"[server][exp {experiment_id}][meas {meas_id}] cycle complete\n")

    print("[server] shutting down")
    sock.close()
    ctx.term()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="ZMQ orchestrator — reads server_config.json only"
    )
    sub = p.add_subparsers(dest="mode", required=True)

    ps = sub.add_parser("server")
    ps.add_argument(
        "--config", default=str(DEFAULT_CONFIG_PATH),
        help="Path to server_config.json  (default: ./server_config.json)",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    server_main(config_path=Path(args.config))


if __name__ == "__main__":
    main()