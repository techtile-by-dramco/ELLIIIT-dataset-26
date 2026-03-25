#!/usr/bin/env python3
"""
test_rover.py

Minimal ZMQ ROUTER that tests only the rover client in isolation.
Sends MOVE and waits for MOVE_DONE, with no dependency on ref, acoustic, or rf.

Usage:
    python test_rover.py
    python test_rover.py --bind tcp://*:5555 --cycles 3 --mov-timeout 30
"""

from __future__ import annotations

import argparse
import json
import logging
import signal
import sys
import time
from typing import Any, Dict, Optional, Tuple

import zmq

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stderr,
)
log = logging.getLogger("test_rover")

def now_ms() -> int:
    return int(time.time() * 1000)


def jdump(obj: Dict[str, Any]) -> bytes:
    return json.dumps(obj, separators=(",", ":"), ensure_ascii=False).encode()


def jload(b: bytes) -> Dict[str, Any]:
    return json.loads(b.decode("utf-8"))

def run(
    bind: str,
    experiment_id: str,
    cycles: int,
    meas_start: int,
    mov_timeout_s: float,
    handshake_timeout_s: float,
    poll_ms: int,
) -> None:
    ctx = zmq.Context.instance()
    sock = ctx.socket(zmq.ROUTER)
    sock.linger = 0
    sock.bind(bind)

    poller = zmq.Poller()
    poller.register(sock, zmq.POLLIN)

    stop = {"flag": False}
    signal.signal(signal.SIGINT, lambda *_: stop.update(flag=True))

    def send(client_id: str, msg: Dict[str, Any]) -> None:
        sock.send_multipart([client_id.encode(), jdump(msg)])
        log.info("→ %-10s  %s", client_id, msg)

    def recv(timeout_ms: int) -> Optional[Tuple[str, Dict[str, Any]]]:
        if not dict(poller.poll(timeout_ms)).get(sock):
            return None
        parts = sock.recv_multipart()
        if len(parts) < 2:
            return None
        cid = parts[0].decode("utf-8", errors="replace")
        msg = jload(parts[-1])
        return cid, msg

    log.info("Waiting for rover HELLO on %s (timeout %.0fs) …", bind, handshake_timeout_s)
    rover_ready = False
    deadline = time.time() + handshake_timeout_s

    while not stop["flag"] and time.time() < deadline:
        got = recv(poll_ms)
        if got is None:
            continue
        cid, msg = got
        log.info("← %-10s  %s", cid, msg)
        if cid == "rover" and msg.get("type") == "HELLO":
            rover_ready = True
            log.info("Rover connected — starting test cycles.")
            break
        else:
            log.warning("Unexpected message from %s before rover HELLO, ignoring.", cid)

    if not rover_ready:
        log.error("Rover did not connect within %.0fs. Aborting.", handshake_timeout_s)
        sock.close()
        ctx.term()
        sys.exit(1)

    passed = 0
    failed = 0
    meas_id = meas_start - 1
    cycle_id = 0

    while not stop["flag"]:
        cycle_id += 1
        if cycles > 0 and cycle_id > cycles:
            break

        meas_id += 1
        log.info("--- cycle %d / meas %d ---", cycle_id, meas_id)

        send("rover", {
            "type":          "MOVE",
            "experiment_id": experiment_id,
            "cycle_id":      cycle_id,
            "meas_id":       meas_id,
            "ts":            now_ms(),
        })

        got_done = False
        move_status = "unknown"
        deadline = time.time() + mov_timeout_s

        while not stop["flag"] and time.time() < deadline and not got_done:
            got = recv(poll_ms)
            if got is None:
                continue
            cid, msg = got
            log.info("← %-10s  %s", cid, msg)

            mtype = msg.get("type")

            if (
                cid == "rover"
                and mtype == "MOVE_DONE"
                and msg.get("experiment_id") == experiment_id
                and msg.get("cycle_id") == cycle_id
                and msg.get("meas_id") == meas_id
            ):
                got_done = True
                move_status = str(msg.get("status", "ok"))

            elif mtype == "HELLO":
                # rover may re-announce; that's fine
                log.info("Rover re-announced HELLO.")

            else:
                log.warning("Unexpected: cid=%s type=%s — ignoring.", cid, mtype)

        if got_done:
            if move_status == "error":
                log.error(
                    "FAIL  cycle=%d meas=%d  rover reported move error: %s",
                    cycle_id, meas_id, move_status,
                )
                failed += 1
            else:
                log.info("PASS  cycle=%d meas=%d  move_status=%s", cycle_id, meas_id, move_status)
                passed += 1
        else:
            log.error(
                "FAIL  cycle=%d meas=%d  timeout waiting for MOVE_DONE (%.0fs)",
                cycle_id, meas_id, mov_timeout_s,
            )
            failed += 1
            # Stop on timeout — rover may be stuck
            break

    total = passed + failed
    log.info("=" * 50)
    log.info("TEST SUMMARY  passed=%d  failed=%d  total=%d", passed, failed, total)
    if failed == 0 and total > 0:
        log.info("All cycles PASSED.")
    elif total == 0:
        log.warning("No cycles completed.")
    else:
        log.error("%d cycle(s) FAILED.", failed)
    log.info("=" * 50)

    sock.close()
    ctx.term()
    sys.exit(0 if failed == 0 else 1)



def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Rover-only ZMQ test harness.")
    p.add_argument("--bind",                default="tcp://*:5555",  help="ZMQ bind address")
    p.add_argument("--experiment-id",       default="TEST001",       help="experiment_id sent in messages")
    p.add_argument("--cycles",              default=5,    type=int,   help="Number of cycles (0 = forever)")
    p.add_argument("--meas-start",          default=1,    type=int,   help="Starting meas_id")
    p.add_argument("--mov-timeout",         default=30.0, type=float, help="Seconds to wait for MOVE_DONE")
    p.add_argument("--handshake-timeout",   default=15.0, type=float, help="Seconds to wait for rover HELLO")
    p.add_argument("--poll-ms",             default=250,  type=int,   help="ZMQ poll interval ms")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run(
        bind=args.bind,
        experiment_id=args.experiment_id,
        cycles=args.cycles,
        meas_start=args.meas_start,
        mov_timeout_s=args.mov_timeout,
        handshake_timeout_s=args.handshake_timeout,
        poll_ms=args.poll_ms,
    )