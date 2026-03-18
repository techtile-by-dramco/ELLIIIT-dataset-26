# python3 client/test_runtime_storage.py --config-file /home/pi/ELLIIIT-dataset-26/experiment-settings.yaml --probe-name smb_probe.txt


import argparse
import logging
import os
import socket
import sys
from datetime import datetime, timezone
from pathlib import Path

import runtime_storage


HOSTNAME_RAW = socket.gethostname()
HOSTNAME = HOSTNAME_RAW[4:] if len(HOSTNAME_RAW) > 4 else HOSTNAME_RAW


class LogFormatter(logging.Formatter):
    @staticmethod
    def pp_now():
        now = datetime.now()
        return "{:%H:%M}:{:05.2f}".format(now, now.second + now.microsecond / 1e6)

    def formatTime(self, record, datefmt=None):
        converter = self.converter(record.created)
        if datefmt:
            return converter.strftime(datefmt)
        return LogFormatter.pp_now()


class ColoredFormatter(LogFormatter):
    COLORS = {
        logging.DEBUG: "\033[36m",
        logging.INFO: "\033[32m",
        logging.WARNING: "\033[33m",
        logging.ERROR: "\033[31m",
        logging.CRITICAL: "\033[35m",
    }
    RESET = "\033[0m"

    def format(self, record):
        color = self.COLORS.get(record.levelno, "")
        reset = self.RESET if color else ""
        record.levelname = f"{color}{record.levelname}{reset}"
        return super().format(record)


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

console = logging.StreamHandler()
formatter = LogFormatter(
    fmt="[%(asctime)s] [%(levelname)s] (%(threadName)-10s) %(message)s"
)
console.setFormatter(ColoredFormatter(fmt=formatter._fmt))
logger.addHandler(console)


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Test runtime SMB storage initialization on a single client."
    )
    parser.add_argument("--config-file", type=str)
    parser.add_argument(
        "--probe-name",
        type=str,
        default=None,
        help="Optional custom file name for the write/read probe.",
    )
    return parser.parse_args()


def build_probe_path(output_dir, probe_name):
    if probe_name:
        return output_dir / probe_name

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return output_dir / f"runtime_storage_probe_{HOSTNAME}_{timestamp}.txt"


def write_probe_file(probe_path, runtime_config):
    payload = "\n".join(
        [
            "runtime_storage_probe: ok",
            f"timestamp_utc: {datetime.now(timezone.utc).isoformat()}",
            f"hostname: {HOSTNAME}",
            f"settings_path: {runtime_config['settings_path']}",
            f"storage_path: {runtime_config['storage_path']}",
            f"host_output_dir: {runtime_config['host_output_dir']}",
            f"pid: {os.getpid()}",
        ]
    ) + "\n"

    with open(probe_path, "w", encoding="utf-8") as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())

    readback = probe_path.read_text(encoding="utf-8")
    if readback != payload:
        raise RuntimeError(f"Probe file verification failed for {probe_path}")


def log_mount_status(storage_path):
    mount_source, _ = runtime_storage.parse_storage_path(storage_path)
    if mount_source is None:
        logger.info("Storage path is local, no SMB mount required.")
        return

    mount_root = Path(
        os.getenv("SMB_MOUNT_ROOT", runtime_storage.DEFAULT_MOUNT_ROOT)
    ).expanduser()
    mount_dir = mount_root / runtime_storage.sanitize_mount_name(mount_source)
    mounted_source = runtime_storage.get_mounted_source(mount_dir)

    logger.info("SMB source: %s", mount_source)
    logger.info("SMB mount dir: %s", mount_dir)
    logger.info("Mounted source: %s", mounted_source or "<not mounted>")


def main():
    args = parse_arguments()
    logger.info("Invocation args: %s", " ".join(sys.argv))

    try:
        runtime_config = runtime_storage.resolve_runtime_output_dir(
            args.config_file,
            HOSTNAME,
        )
    except Exception as exc:
        logger.error("Runtime storage initialization failed: %s", exc)
        return 1

    logger.info("Resolved hostname: %s", HOSTNAME)
    logger.info("Loaded experiment settings from %s", runtime_config["settings_path"])
    logger.info("Configured storage path: %s", runtime_config["storage_path"])
    logger.info("Host output directory: %s", runtime_config["host_output_dir"])
    log_mount_status(runtime_config["storage_path"])

    probe_path = build_probe_path(runtime_config["host_output_dir"], args.probe_name)
    try:
        write_probe_file(probe_path, runtime_config)
    except Exception as exc:
        logger.error("Probe write/read check failed: %s", exc)
        return 2

    logger.info("Probe file created and verified: %s", probe_path)
    print("OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
