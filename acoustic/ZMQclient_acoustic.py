from acousticMeasurement import load_config, read_system, run_acoustic_measurement
import zmq
import json
import time

def now_ms() -> int:
    return int(time.time() * 1000)

def measurer_client(connect: str, client_id: str):
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

    print(f"[{client_id}] Connected to {connect}. Waiting for commands...")

    # Announce presence
    sock.send(json.dumps({"type": "HELLO", "id": client_id, "ts": now_ms()}).encode())

    while True:
        try:
            # Poll for messages
            events = dict(poller.poll(timeout=1000))
            if sock not in events:
                continue

            msg = json.loads(sock.recv().decode("utf-8"))
            mtype = msg.get("type")
            
            if mtype == "START_MEAS":
                exp_id = msg.get("experiment_id")
                meas_id = msg.get("meas_id")
                cycle_id = msg.get("cycle_id")

                print(f"[{client_id}] Starting measurement for Exp:{exp_id} Meas:{meas_id}")

                # --- THE ACTUAL MEASUREMENT ---
                overrides = {
                    k: msg[k]
                    for k in (
                        "speaker_coordinates",
                        "chirp_f_start", "chirp_f_stop",
                        "chirp_duration", "chirp_DC", "chirp_ampl",
                    )
                    if k in msg
                }
                
                try:
                    result = run_acoustic_measurement(base_config, overrides)
                    response = {
                        "type":          "MEAS_DONE",
                        "experiment_id": exp_id,
                        "meas_id":       meas_id,
                        "cycle_id":      cycle_id,
                        "id":            client_id,
                        "status":        "ok",
                        "csv_file":      result["csv_file"],
                        "n_mics":        result["n_mics"],
                        "duration_s":    result["duration_s"]
                    }
                    print(
                        f"[{client_id}] MEAS_DONE  "
                        f"mics={result['n_mics']}  "
                        f"took={result['duration_s']}s  "
                        f"file={result['csv_file']}"
                    )

                except Exception as exc:
                    response = {
                        "type":          "MEAS_DONE",
                        "experiment_id": exp_id,
                        "meas_id":       meas_id,
                        "cycle_id":      cycle_id,
                        "id":            client_id,
                        "status":        "error",
                        "error":         str(exc),
                    }
                    print(f"[{client_id}] ERROR during measurement: {exc}")
                # ------------------------------
                sock.send(json.dumps(response).encode())
                print(f"[{client_id}] Sent result: {response}")

            elif mtype == "PING":
                sock.send(json.dumps({"type": "PONG", "ts": now_ms()}).encode())

        except KeyboardInterrupt:
            print(f"[{client_id}] Shutting down.")
            break

    sock.close()
    ctx.term()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--connect", default="tcp://127.0.0.1:5555")
    parser.add_argument("--id", required=True, choices=["meas1", "meas2"])
    args = parser.parse_args()

    measurer_client(args.connect, args.id)