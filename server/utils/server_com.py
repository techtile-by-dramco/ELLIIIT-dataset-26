import zmq
import json
import time
import signal
import threading
from datetime import datetime, timedelta

class Server:
    def __init__(self, msg_port="5678", sync_port="5679", heartbeat_timeout=10, silent=False):
        self.context = zmq.Context()
        self.messaging = self.context.socket(zmq.ROUTER)
        self.messaging.bind(f"tcp://*:{msg_port}")
        self.sync = self.context.socket(zmq.PUB)
        self.sync.bind(f"tcp://*:{sync_port}")
        self.clients = {}
        self.heartbeat_timeout = heartbeat_timeout
        self.silent = silent
        self.running = True
        self.thread = None
        # Event handling
        self.callbacks = {}

    def start(self):
        """Start the server in a background thread."""
        if self.thread is not None:
            return  # already running

        self.thread = threading.Thread(target=self.run, daemon=True)
        self.thread.start()

    def stop(self):
        """Ask the server loop to stop."""
        self.running = False

    def join(self):
        """Wait for the server thread to finish."""
        if self.thread is not None:
            self.thread.join()

    def on(self, msg_type, func):
        """Register a callback for a given server command."""
        self.callbacks[msg_type] = func

    def _cleanup(self):
        """Close resources cleanly."""
        print("\nShutting down server...")

        try:
            self.messaging.close(linger=0)
            self.sync.close(linger=0)
        except Exception:
            pass

        try:
            self.context.term()
        except Exception:
            pass

        print("Server stopped cleanly.")

    def run(self):
        print("Server running... waiting for clients (Ctrl+C or Ctrl+Z to stop)")

        poller = zmq.Poller()
        poller.register(self.messaging, zmq.POLLIN)

        try:
            while self.running:
                try:
                    messages = dict(poller.poll(1000))  # may be interrupted
                except zmq.error.ZMQError:
                    break
                except KeyboardInterrupt:
                    # KeyboardInterrupt raised during poll()
                    self.running = False
                    break

                if self.messaging in messages:
                    frames = self.messaging.recv_multipart()
                    if not frames:
                        continue

                    identity, *payload = frames

                    # First payload frame is the message type
                    msg_type = payload[0].decode() if len(payload) >= 1 else ""
                    msg_payload = payload[1:] if len(payload) > 1 else []

                    # Update last_seen
                    self.clients[identity] = {"last_seen": datetime.utcnow()}

                    # Handle messages
                    if msg_type == "heartbeat":
                        if not self.silent:
                            print(f"[HEARTBEAT] {identity.decode()}")
                    else:
                        if not self.silent:
                            print(f"[MESSAGE] {identity.decode()}: {msg_payload}")
                        if msg_type in self.callbacks:
                            try:
                                self.callbacks[msg_type](identity.decode(), msg_payload)
                            except Exception as e:
                                print(f"Callback error for {msg_type}: {e}")
                        else:
                            print("unhandled message")

                self._purge_dead()

        except KeyboardInterrupt:
            # Interrupt outside poll, e.g. between iterations
            pass
        finally:
            self._cleanup()

    def _purge_dead(self):
        now = datetime.utcnow()
        dead = []
        for cid, info in list(self.clients.items()):
            if now - info["last_seen"] > timedelta(seconds=self.heartbeat_timeout):
                dead.append(cid)
        for cid in dead:
            if not self.silent:
                print(f"[TIMEOUT] Removing client {cid.decode()}")
            del self.clients[cid]
                
    def print_clients(self, short=False):
        if len(self.clients) == 0:
            print("no connected clients")
        else:
            if short:
                cids = sorted(self.clients)
                cidstr = ""
                for cid in cids:
                    if len(cidstr) > 0:
                        cidstr += " "
                    cidstr += cid.decode();
                print(cidstr)
            else:
                print("connected clients:")
                for cid, info in list(self.clients.items()):
                    print(cid, "- last seen:", info["last_seen"])

    def get_connected(self):
        return self.clients

    def send(self, client_id, msg_type, *payload_frames):
        """
        Send a message to a specific connected client.

        Parameters
        ----------
        client_id : bytes
            The ROUTER identity of the client.
        msg_type : str
            Message type string (e.g., 'command', 'ping', etc.).
        payload_frames : list of bytes or str
            Optional additional frames after the message type.
        """
        if client_id not in self.clients:
            raise ValueError(f"Client {client_id!r} is not connected.")

        # Build multipart message
        frames = [client_id, msg_type.encode()]
        for frame in payload_frames:
            if isinstance(frame, str):
                frame = frame.encode()
            frames.append(frame)

        self.messaging.send_multipart(frames)

    def broadcast(self, msg_type, *payload_frames):
        frames = [msg_type.encode()]

        for f in payload_frames:
            if isinstance(f, str):
                f = f.encode()
            frames.append(f)

        self.sync.send_multipart(frames)
