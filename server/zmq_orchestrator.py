#!/usr/bin/env python3
"""
zmq_orchestrator.py

Single-file ZMQ orchestrator with:
- Server (ROUTER): runs an experiment with a stable experiment_id and incremental meas_id
- Clients (DEALER): roles in {"meas1","meas2","mover"} that execute commands and reply with matching IDs

Cycle:
  server -> START_MEAS (meas1, meas2) with {experiment_id, meas_id}
  meas1/meas2 -> MEAS_DONE with {experiment_id, meas_id}
  server -> START_MOV (mover) with {experiment_id, meas_id}
  mover -> DONE_MOV with {experiment_id, meas_id}
  repeat (meas_id increments each cycle)

Run 4 terminals:

1) Server:
   python zmq_orchestrator.py server --bind tcp://*:5555 --experiment-id EXP001

2) Measurement clients:
   python zmq_orchestrator.py client --connect tcp://127.0.0.1:5555 --id meas1
   python zmq_orchestrator.py client --connect tcp://127.0.0.1:5555 --id meas2

3) Mover client:
   python zmq_orchestrator.py client --connect tcp://127.0.0.1:5555 --id mover

Optional:
- deterministic simulated durations:
   python zmq_orchestrator.py client --id meas1 --meas-time 0.5
   python zmq_orchestrator.py client --id mover --mov-time 0.3

Notes:
- Messages are JSON.
- Server validates experiment_id + meas_id on replies (ignores out-of-cycle messages).
"""

from __future__ import annotations

import argparse
import json
import random
import signal
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional, Set, Tuple

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
    mov_s: float = 10.0
    poll_ms: int = 250


# -----------------------------
# Server (ROUTER)
# -----------------------------
def server_main(
    bind: str,
    cycles: int,
    timeouts: Timeouts,
    experiment_id: str,
    meas_start: int,
) -> None:
    ctx = zmq.Context.instance()
    sock = ctx.socket(zmq.ROUTER)
    sock.linger = 0
    sock.bind(bind)

    poller = zmq.Poller()
    poller.register(sock, zmq.POLLIN)

    alive: Set[str] = set()
    needed = {"meas1", "meas2", "mover"}

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
    print(
        "[server] waiting briefly for HELLO from meas1, meas2, mover (Ctrl+C to stop)"
    )

    # Optional: wait a bit for HELLOs
    t0 = time.time()
    while not stop["flag"] and (alive != needed) and (time.time() - t0 < 5.0):
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
            f"[server] warning: not all clients present. alive={sorted(alive)} needed={sorted(needed)}"
        )
        print("[server] continuing anyway; missing clients will cause timeouts.\n")

    cycle_id = 0
    meas_id = meas_start - 1  # first increment yields meas_start

    while not stop["flag"]:
        cycle_id += 1
        if cycles > 0 and cycle_id > cycles:
            break

        meas_id += 1

        # ---- START_MEAS to both measurement clients
        meas_done: Set[str] = set()
        start_meas_msg = {
            "type": "START_MEAS",
            "experiment_id": experiment_id,
            "cycle_id": cycle_id,
            "meas_id": meas_id,
            "ts": now_ms(),
        }
        for meas in ("meas1", "meas2"):
            print(f"[server][exp {experiment_id}][meas {meas_id}] -> {meas} START_MEAS")
            send_to(meas, start_meas_msg)

        # ---- Wait for both MEAS_DONE (must match experiment_id+meas_id+cycle_id)
        deadline = time.time() + timeouts.meas_s
        while (
            not stop["flag"]
            and time.time() < deadline
            and meas_done != {"meas1", "meas2"}
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
                and cid in {"meas1", "meas2"}
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

        if meas_done != {"meas1", "meas2"}:
            missing = {"meas1", "meas2"} - meas_done
            print(
                f"[server][exp {experiment_id}][meas {meas_id}] TIMEOUT waiting MEAS_DONE. missing={sorted(missing)}"
            )
            break

        # ---- START_MOV to mover
        print(f"[server][exp {experiment_id}][meas {meas_id}] -> mover START_MOV")
        send_to(
            "mover",
            {
                "type": "START_MOV",
                "experiment_id": experiment_id,
                "cycle_id": cycle_id,
                "meas_id": meas_id,
                "ts": now_ms(),
            },
        )

        # ---- Wait for DONE_MOV (must match experiment_id+meas_id+cycle_id)
        got_done = False
        deadline = time.time() + timeouts.mov_s
        while not stop["flag"] and time.time() < deadline and not got_done:
            got = recv_one(timeout_ms=timeouts.poll_ms)
            if got is None:
                continue
            cid, msg = got
            mtype = msg.get("type")
            mid_exp = msg.get("experiment_id")
            mid_meas = msg.get("meas_id")
            mid_cycle = msg.get("cycle_id")

            if (
                cid == "mover"
                and mtype == "DONE_MOV"
                and mid_exp == experiment_id
                and mid_meas == meas_id
                and mid_cycle == cycle_id
            ):
                got_done = True
                print(
                    f"[server][exp {experiment_id}][meas {meas_id}] <- mover DONE_MOV"
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

        if not got_done:
            print(
                f"[server][exp {experiment_id}][meas {meas_id}] TIMEOUT waiting DONE_MOV"
            )
            break

        print(f"[server][exp {experiment_id}][meas {meas_id}] cycle complete\n")

    print("[server] shutting down")
    sock.close()
    ctx.term()


# -----------------------------
# Client (DEALER)
# -----------------------------
def client_main(
    connect: str, client_id: str, meas_time_s: float, mov_time_s: float
) -> None:
    if client_id not in {"meas1", "meas2", "mover"}:
        raise ValueError("client --id must be one of: meas1, meas2, mover")

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

    # announce presence (no experiment_id here on purpose)
    send({"type": "HELLO", "id": client_id, "ts": now_ms()})
    print(f"[client:{client_id}] connected to {connect}")

    while not stop["flag"]:
        msg = recv(timeout_ms=1000)
        if msg is None:
            continue

        mtype = msg.get("type")
        experiment_id = msg.get("experiment_id")
        cycle_id = msg.get("cycle_id")
        meas_id = msg.get("meas_id")

        if mtype == "START_MEAS":
            if client_id not in {"meas1", "meas2"}:
                continue

            print(
                f"[client:{client_id}][exp {experiment_id}][meas {meas_id}] START_MEAS"
            )

            # Replace with real measurement work
            t = meas_time_s if meas_time_s > 0 else random.uniform(0.2, 1.0)
            time.sleep(t)

            send(
                {
                    "type": "MEAS_DONE",
                    "experiment_id": experiment_id,
                    "cycle_id": cycle_id,
                    "meas_id": meas_id,
                    "id": client_id,
                    "ts": now_ms(),
                    "duration_s": round(t, 6),
                }
            )
            print(
                f"[client:{client_id}][exp {experiment_id}][meas {meas_id}] MEAS_DONE (t={t:.2f}s)"
            )

        elif mtype == "START_MOV":
            if client_id != "mover":
                continue

            print(
                f"[client:{client_id}][exp {experiment_id}][meas {meas_id}] START_MOV"
            )

            # Replace with real motion work
            t = mov_time_s if mov_time_s > 0 else random.uniform(0.2, 1.0)
            time.sleep(t)

            send(
                {
                    "type": "DONE_MOV",
                    "experiment_id": experiment_id,
                    "cycle_id": cycle_id,
                    "meas_id": meas_id,
                    "id": client_id,
                    "ts": now_ms(),
                    "duration_s": round(t, 6),
                }
            )
            print(
                f"[client:{client_id}][exp {experiment_id}][meas {meas_id}] DONE_MOV (t={t:.2f}s)"
            )

        elif mtype == "PING":
            send({"type": "PONG", "ts": now_ms()})

        else:
            send(
                {
                    "type": "ERROR",
                    "experiment_id": experiment_id,
                    "cycle_id": cycle_id,
                    "meas_id": meas_id,
                    "id": client_id,
                    "error": f"Unknown type {mtype}",
                    "ts": now_ms(),
                }
            )

    print(f"[client:{client_id}] shutting down")
    sock.close()
    ctx.term()


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
    ps.add_argument("--mov-timeout", type=float, default=10.0)
    ps.add_argument("--poll-ms", type=int, default=250)

    pc = sub.add_parser("client")
    pc.add_argument("--connect", default="tcp://127.0.0.1:5555")
    pc.add_argument("--id", required=True, choices=["meas1", "meas2", "mover"])
    pc.add_argument(
        "--meas-time", type=float, default=0.0, help="0 = random simulated duration"
    )
    pc.add_argument(
        "--mov-time", type=float, default=0.0, help="0 = random simulated duration"
    )

    return p.parse_args()


def main() -> None:
    args = parse_args()
    if args.mode == "server":
        server_main(
            bind=args.bind,
            cycles=args.cycles,
            timeouts=Timeouts(
                meas_s=args.meas_timeout, mov_s=args.mov_timeout, poll_ms=args.poll_ms
            ),
            experiment_id=args.experiment_id,
            meas_start=args.meas_start,
        )
    else:
        client_main(
            connect=args.connect,
            client_id=args.id,
            meas_time_s=args.meas_time,
            mov_time_s=args.mov_time,
        )


if __name__ == "__main__":
    main()
