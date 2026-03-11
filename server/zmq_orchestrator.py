#!/usr/bin/env python3
"""
zmq_orchestrator.py

Single-file ZMQ orchestrator with:
- Server (ROUTER): runs an experiment with a stable experiment_id and incremental meas_id
- Clients (DEALER): pass

Cycle:
  server -> START_MEAS (meas1) with {experiment_id, meas_id}
  meas1 -> MEAS_DONE with {experiment_id, meas_id}
  repeat (meas_id increments each cycle)

Run 2 terminals:

1) Server:
   python zmq_orchestrator.py server --bind tcp://*:5555 --experiment-id EXP001

2) Measurement client:
   python zmqclient_acoustic.py --connect tcp://127.0.0.1:5555 --id meas1

Optional:
- deterministic simulated durations:
   pass

- per-cycle measurement parameters (NEW):
   python zmq_orchestrator.py server --experiment-id EXP001 --meas-plan measureConfig.json
   measureConfig.json: list of dicts, one per cycle, e.g.:
   [
     {"speaker_coordinates": [1.0, 0.0, 1.2], "chirp_f_start": 200, "chirp_f_stop": 8000,
      "chirp_duration": 3.0, "chirp_DC": 0, "chirp_ampl": 0.5},
     {"speaker_coordinates": [2.0, 0.5, 1.2]}
   ]
   Last entry repeats if cycles > len(plan).

"""

from __future__ import annotations

import argparse
import json
import random
import signal
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set, Tuple

import zmq


def now_ms() -> int:
    return int(time.time() * 1000)


def jdump(obj: Dict[str, Any]) -> bytes:
    return json.dumps(obj, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def jload(b: bytes) -> Dict[str, Any]:
    return json.loads(b.decode("utf-8"))


@dataclass
class Timeouts:
    meas_s: float = 10.0
    poll_ms: int = 250


# Server
def server_main(
    bind: str,
    cycles: int,
    timeouts: Timeouts,
    experiment_id: str,
    meas_start: int,
    meas_plan: List[Dict[str, Any]],
) -> None:
    ctx = zmq.Context.instance()
    sock = ctx.socket(zmq.ROUTER)
    sock.linger = 0
    sock.bind(bind)

    poller = zmq.Poller()
    poller.register(sock, zmq.POLLIN)

    alive: Set[str] = set()
    needed = {"meas1"}

    stop = {"flag": False}

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
    print(f"[server] experiment_id={experiment_id} meas_start={meas_start}")
    print("[server] waiting briefly for HELLO from meas1 (Ctrl+C to stop)")

    t0 = time.time()
    while not stop["flag"] and (alive != needed) and (time.time() - t0 < 15.0): # Wait 15 seconds
        got = recv_one(timeout_ms=timeouts.poll_ms)
        if got is None:
            continue
        cid, msg = got
        if msg.get("type") == "HELLO":
            alive.add(cid)
            print(f"[server] HELLO from {cid} (alive={sorted(alive)})")
        else:
            print(f"[server] (pre-loop) got {msg.get('type')} from {cid}: {msg}")

    if alive != needed:
        print(
            f"[server] warning: not all clients present"
        )
        print("[server] continuing anyway; missing clients will cause timeouts.\n")

    cycle_id = 0
    meas_id = meas_start - 1

    while not stop["flag"]:
        cycle_id += 1
        if cycles > 0 and cycle_id > cycles:
            break

        meas_id += 1

        # NEW: pick measurement params for this cycle (last entry repeats if plan exhausted)
        cycle_params: Dict[str, Any] = {}
        if meas_plan:
            idx = min(cycle_id - 1, len(meas_plan) - 1)
            cycle_params = meas_plan[idx]

        # ---- START_MEAS to meas1
        meas_done: Set[str] = set()
        start_meas_msg = {
            "type": "START_MEAS",
            "experiment_id": experiment_id,
            "cycle_id": cycle_id,
            "meas_id": meas_id,
            "ts": now_ms(),
            **cycle_params,
        }
        print(f"[server][exp {experiment_id}][meas {meas_id}] -> meas1 START_MEAS")
        send_to("meas1", start_meas_msg)

        # ---- Wait for MEAS_DONE (must match experiment_id+meas_id+cycle_id)
        deadline = time.time() + timeouts.meas_s
        while (
            not stop["flag"]
            and time.time() < deadline
            and meas_done != {"meas1"}
        ):
            got = recv_one(timeout_ms=timeouts.poll_ms)
            if got is None:
                continue
            cid, msg = got
            mtype = msg.get("type")
            mid_exp = msg.get("experiment_id")
            mid_meas = msg.get("meas_id")
            mid_cycle = msg.get("cycle_id")

            if (
                mtype == "MEAS_DONE"
                and cid in {"meas1"}
                and mid_exp == experiment_id
                and mid_meas == meas_id
                and mid_cycle == cycle_id
            ):
                meas_done.add(cid)
                print(
                    f"[server][exp {experiment_id}][meas {meas_id}] <- {cid} MEAS_DONE (done={sorted(meas_done)})"
                )
            elif mtype == "ERROR" and mid_exp == experiment_id and mid_meas == meas_id:
                print(
                    f"[server][exp {experiment_id}][meas {meas_id}] <- {cid} ERROR: {msg.get('error')}"
                )
            elif mtype == "HELLO":
                alive.add(cid)
            else:
                print(
                    f"[server][exp {experiment_id}][meas {meas_id}] (ignored) <- {cid} {mtype} "
                    f"exp={mid_exp} meas={mid_meas} cycle={mid_cycle}"
                )

        if meas_done != {"meas1"}:
            print(
                f"[server][exp {experiment_id}][meas {meas_id}] TIMEOUT waiting MEAS_DONE."
            )
            break

        print(f"[server][exp {experiment_id}][meas {meas_id}] cycle complete\n")

    print("[server] shutting down")
    sock.close()
    ctx.term()


# -----------------------------
# Client (DEALER)
# -----------------------------
def client_main():
    pass


# -----------------------------
# CLI
# -----------------------------
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="mode", required=True)

    ps = sub.add_parser("server")
    ps.add_argument("--bind", default="tcp://*:5555")
    ps.add_argument(
        "--experiment-id", required=True, help="Stable ID for the whole experiment/run"
    )
    ps.add_argument(
        "--meas-start", type=int, default=1, help="Starting MEAS ID counter"
    )
    ps.add_argument("--cycles", type=int, default=0, help="0 = run forever")
    ps.add_argument("--meas-timeout", type=float, default=10.0)
    ps.add_argument("--poll-ms", type=int, default=250)
    ps.add_argument(
        "--meas-plan", default=None, metavar="FILE",
        help="JSON file: list of per-cycle measurement-parameter dicts "
             "(speaker_coordinates, chirp_f_start, chirp_f_stop, "
             "chirp_duration, chirp_DC, chirp_ampl)"
    )

    pc = sub.add_parser("client")
    pc.add_argument("--connect", default="tcp://127.0.0.1:5555")
    pc.add_argument("--id", required=True, choices=["meas1"])
    pc.add_argument(
        "--meas-time", type=float, default=0.0, help="0 = random simulated duration"
    )

    return p.parse_args()


def main() -> None:
    args = parse_args()
    if args.mode == "server":
        meas_plan: List[Dict[str, Any]] = []
        if args.meas_plan:
            with open(args.meas_plan, "r", encoding="utf-8") as fh:
                meas_plan = json.load(fh)
            print(f"[server] loaded meas-plan: {len(meas_plan)} entries from {args.meas_plan}")

        server_main(
            bind=args.bind,
            cycles=args.cycles,
            timeouts=Timeouts(
                meas_s=args.meas_timeout, poll_ms=args.poll_ms
            ),
            experiment_id=args.experiment_id,
            meas_start=args.meas_start,
            meas_plan=meas_plan,
        )
    else:
        client_main()


if __name__ == "__main__":
    main()