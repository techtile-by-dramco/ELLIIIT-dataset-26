import os
import re
import shutil
import subprocess
from pathlib import Path

import yaml


DEFAULT_SETTINGS_FILE = "experiment-settings.yaml"
DEFAULT_MOUNT_ROOT = Path.home() / "mnt" / "experiment-storage"


def resolve_runtime_output_dir(config_file, hostname):
    settings_path = resolve_settings_path(config_file)
    settings = load_settings(settings_path)

    experiment_config = settings.get("experiment_config") or {}
    storage_path = experiment_config.get("storage_path")
    if not storage_path:
        raise ValueError(
            f"Missing 'experiment_config.storage_path' in {settings_path}"
        )

    base_output_dir = prepare_storage_base(
        storage_path=storage_path,
        settings_path=settings_path,
    )
    host_output_dir = base_output_dir / hostname
    host_output_dir.mkdir(parents=True, exist_ok=True)

    return {
        "settings_path": settings_path,
        "storage_path": storage_path,
        "host_output_dir": host_output_dir,
    }


def resolve_settings_path(config_file):
    if config_file:
        return Path(config_file).expanduser().resolve()
    return Path(__file__).resolve().parents[1] / DEFAULT_SETTINGS_FILE


def load_settings(settings_path):
    try:
        with open(settings_path, "r", encoding="utf-8") as file:
            return yaml.safe_load(file) or {}
    except FileNotFoundError as exc:
        raise FileNotFoundError(
            f"Experiment settings file not found: {settings_path}"
        ) from exc
    except yaml.YAMLError as exc:
        raise ValueError(
            f"Unable to parse experiment settings file {settings_path}: {exc}"
        ) from exc


def prepare_storage_base(storage_path, settings_path):
    source, relative_parts = parse_storage_path(storage_path)
    if source is None:
        local_base = Path(storage_path).expanduser()
        if not local_base.is_absolute():
            local_base = (settings_path.parent / local_base).resolve()
        local_base.mkdir(parents=True, exist_ok=True)
        return local_base

    mount_root = Path(os.getenv("SMB_MOUNT_ROOT", DEFAULT_MOUNT_ROOT)).expanduser()
    mount_dir = mount_root / sanitize_mount_name(source)
    ensure_cifs_mount(source, mount_dir)

    storage_base = mount_dir.joinpath(*relative_parts)
    storage_base.mkdir(parents=True, exist_ok=True)
    return storage_base


def parse_storage_path(storage_path):
    normalized = storage_path.strip()
    if normalized.startswith("\\\\") or normalized.startswith("//"):
        trimmed = normalized.lstrip("\\/")
        parts = [part for part in re.split(r"[\\/]+", trimmed) if part]
        if len(parts) < 2:
            raise ValueError(
                "SMB storage_path must include a server and share name, "
                f"got: {storage_path}"
            )
        server, share, *relative_parts = parts
        return f"//{server}/{share}", relative_parts
    return None, []


def sanitize_mount_name(source):
    return re.sub(r"[^A-Za-z0-9._-]+", "_", source.lstrip("/"))


def ensure_cifs_mount(source, mount_dir):
    mount_dir.mkdir(parents=True, exist_ok=True)
    mounted_source = get_mounted_source(mount_dir)
    if mounted_source:
        if mounted_source == source:
            return
        raise RuntimeError(
            f"Mount point {mount_dir} is already used for {mounted_source}, "
            f"expected {source}"
        )

    mount_cmd = [
        "mount",
        "-t",
        "cifs",
        source,
        str(mount_dir),
        "-o",
        build_mount_options(),
    ]

    errors = []
    for command in candidate_mount_commands(mount_cmd):
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode == 0:
            if get_mounted_source(mount_dir) == source:
                return
            raise RuntimeError(
                f"Mounted {source} on {mount_dir}, but verification failed."
            )
        errors.append(format_mount_error(command, result))

    raise RuntimeError(
        f"Failed to mount SMB share {source} on {mount_dir}.\n"
        + "\n".join(errors)
    )


def candidate_mount_commands(mount_cmd):
    commands = [mount_cmd]
    if os.geteuid() != 0 and shutil.which("sudo"):
        commands.append(["sudo", "-n", *mount_cmd])
    return commands


def build_mount_options():
    override = os.getenv("SMB_MOUNT_OPTIONS")
    if override:
        return override

    options = [
        "rw",
        "vers=3.0",
        f"uid={os.getuid()}",
        f"gid={os.getgid()}",
        "file_mode=0664",
        "dir_mode=0775",
    ]

    credentials_file = os.getenv("SMB_CREDENTIALS_FILE")
    if credentials_file:
        options.append(f"credentials={credentials_file}")
    else:
        username = os.getenv("SMB_USERNAME")
        password = os.getenv("SMB_PASSWORD")
        domain = os.getenv("SMB_DOMAIN")

        if username:
            options.append(f"username={username}")
            if password is not None:
                options.append(f"password={password}")
            if domain:
                options.append(f"domain={domain}")
        else:
            options.append("guest")

    return ",".join(options)


def get_mounted_source(mount_dir):
    mount_dir_str = str(mount_dir)
    try:
        with open("/proc/mounts", "r", encoding="utf-8") as mounts:
            for line in mounts:
                parts = line.split()
                if len(parts) >= 2 and parts[1] == mount_dir_str:
                    return parts[0]
    except FileNotFoundError:
        return None
    return None


def format_mount_error(command, result):
    stderr = (result.stderr or "").strip()
    stdout = (result.stdout or "").strip()
    details = stderr or stdout or f"exit code {result.returncode}"
    return f"{' '.join(command)} -> {details}"
