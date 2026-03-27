#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "$SCRIPT_DIR/.." && pwd)"
SERVER_CONFIG="${SERVER_CONFIG:-$SCRIPT_DIR/serverConfig.yaml}"
EXPERIMENT_SETTINGS="${EXPERIMENT_SETTINGS:-$REPO_ROOT/experiment-settings.yaml}"

DRY_RUN=0

usage() {
    cat <<EOF
Usage: $(basename "$0") [--dry-run]

Force-stop local processes that own this repository's ZMQ sockets.

Files checked for bind ports:
  - server/zmq_orchestrator.py
  - server/record/RF-orchestrator.py
  - server/record/sync-server.py
  - server/run_server.py (via server/utils/server_com.py)
  - client/rover/ZMQserverTest_rover.py
  - client/run_reciprocity.py
  - client/usrp_pilot.py

Environment overrides:
  SERVER_CONFIG=/path/to/serverConfig.yaml
  EXPERIMENT_SETTINGS=/path/to/experiment-settings.yaml
EOF
}

log() {
    printf '[close-sockets] %s\n' "$*"
}

die() {
    log "$*" >&2
    exit 1
}

while (($# > 0)); do
    case "$1" in
        --dry-run)
            DRY_RUN=1
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            usage >&2
            die "Unknown argument: $1"
            ;;
    esac
done

declare -A PORT_REASONS=()
declare -A PID_REASONS=()

add_port() {
    local port="$1"
    local reason="$2"

    [[ "$port" =~ ^[0-9]+$ ]] || return 0

    if [[ -n "${PORT_REASONS[$port]:-}" ]]; then
        PORT_REASONS["$port"]+=", $reason"
    else
        PORT_REASONS["$port"]="$reason"
    fi
}

add_pid() {
    local pid="$1"
    local reason="$2"

    [[ "$pid" =~ ^[0-9]+$ ]] || return 0
    [[ "$pid" -eq "$$" || "$pid" -eq "$PPID" ]] && return 0

    if [[ -n "${PID_REASONS[$pid]:-}" ]]; then
        PID_REASONS["$pid"]+=", $reason"
    else
        PID_REASONS["$pid"]="$reason"
    fi
}

load_ports_from_config() {
    command -v python3 >/dev/null 2>&1 || return 0

    python3 - "$EXPERIMENT_SETTINGS" "$SERVER_CONFIG" <<'PY'
import pathlib
import re
import sys

exp_path = pathlib.Path(sys.argv[1])
server_path = pathlib.Path(sys.argv[2])


def emit(key, value):
    if value:
        print(f"{key}={value}")


if server_path.exists():
    text = server_path.read_text(encoding="utf-8", errors="ignore")
    match = re.search(r'^\s*bind\s*:\s*["\']?tcp://[^:\s"\']+:(\d+)["\']?', text, re.M)
    emit("server_bind_port", match.group(1) if match else None)


if exp_path.exists():
    section = None
    for raw_line in exp_path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = raw_line.split("#", 1)[0].rstrip()
        if not line.strip():
            continue

        if not raw_line.startswith((" ", "\t")):
            section = None
            if re.match(r"^server:\s*$", line):
                section = "server"
            elif re.match(r"^rf_sync:\s*$", line):
                section = "rf_sync"
            continue

        if section is None:
            continue

        match = re.match(r'^\s{2}([A-Za-z_][\w-]*)\s*:\s*(.+?)\s*$', line)
        if not match:
            continue

        key, value = match.groups()
        value = value.strip().strip('\'"')

        if section == "server":
            if key == "orchestrator_port":
                emit("orchestrator_port", value)
            elif key == "messaging_port":
                emit("messaging_port", value)
            elif key == "sync_port":
                emit("server_sync_port", value)
        elif section == "rf_sync":
            if key == "sync_port":
                emit("rf_sync_port", value)
            elif key == "alive_port":
                emit("rf_alive_port", value)
            elif key == "done_port":
                emit("rf_done_port", value)
PY
}

pids_for_port() {
    local port="$1"

    if command -v lsof >/dev/null 2>&1; then
        lsof -tiTCP:"$port" -sTCP:LISTEN 2>/dev/null || true
        return 0
    fi

    if command -v fuser >/dev/null 2>&1; then
        fuser -n tcp "$port" 2>/dev/null | tr ' ' '\n' | sed '/^$/d' || true
        return 0
    fi

    if command -v ss >/dev/null 2>&1; then
        ss -ltnp "sport = :$port" 2>/dev/null \
            | sed -n 's/.*pid=\([0-9]\+\).*/\1/p' \
            | sort -u || true
        return 0
    fi
}

add_port 5555 "default orchestrator port"
add_port 5557 "default RF sync port"
add_port 5558 "default RF alive port"
add_port 5559 "default RF done port"
add_port 5678 "default server messaging port"
add_port 5679 "default server sync port"
add_port 50001 "default RF IQ PUB port"

while IFS='=' read -r key value; do
    case "$key" in
        server_bind_port)
            add_port "$value" "serverConfig.yaml bind"
            ;;
        orchestrator_port)
            add_port "$value" "experiment-settings.yaml server.orchestrator_port"
            ;;
        messaging_port)
            add_port "$value" "experiment-settings.yaml server.messaging_port"
            ;;
        server_sync_port)
            add_port "$value" "experiment-settings.yaml server.sync_port"
            ;;
        rf_sync_port)
            add_port "$value" "experiment-settings.yaml rf_sync.sync_port"
            ;;
        rf_alive_port)
            add_port "$value" "experiment-settings.yaml rf_sync.alive_port"
            ;;
        rf_done_port)
            add_port "$value" "experiment-settings.yaml rf_sync.done_port"
            ;;
    esac
done < <(load_ports_from_config)

PROCESS_PATTERNS=(
    "server/zmq_orchestrator.py"
    "server/record/RF-orchestrator.py"
    "server/record/sync-server.py"
    "server/run_server.py"
    "client/rover/ZMQserverTest_rover.py"
    "client/rover/ZMQclient_rover.py"
    "acoustic/ZMQclient_acoustic.py"
    "client/run-ref.py"
    "client/run_reciprocity.py"
    "client/run_uncalibrated.py"
    "client/usrp_pilot.py"
)

for pattern in "${PROCESS_PATTERNS[@]}"; do
    while IFS= read -r pid; do
        add_pid "$pid" "process match: $pattern"
    done < <(pgrep -f -- "$pattern" || true)
done

while IFS= read -r port; do
    while IFS= read -r pid; do
        add_pid "$pid" "port $port (${PORT_REASONS[$port]})"
    done < <(pids_for_port "$port")
done < <(printf '%s\n' "${!PORT_REASONS[@]}" | sort -n)

if [[ "${#PID_REASONS[@]}" -eq 0 ]]; then
    log "No matching local ZMQ processes or bind ports found."
    exit 0
fi

log "Ports checked:"
while IFS= read -r port; do
    log "  $port -> ${PORT_REASONS[$port]}"
done < <(printf '%s\n' "${!PORT_REASONS[@]}" | sort -n)

log "Matching processes:"
while IFS= read -r pid; do
    reason="${PID_REASONS[$pid]}"
    details="$(ps -o pid=,ppid=,stat=,args= -p "$pid" 2>/dev/null || true)"
    if [[ -n "$details" ]]; then
        log "  $details"
        log "    reason: $reason"
    else
        log "  pid=$pid reason: $reason"
    fi
done < <(printf '%s\n' "${!PID_REASONS[@]}" | sort -n)

if (( DRY_RUN )); then
    log "Dry run only. No signals sent."
    exit 0
fi

log "Sending CONT to resume suspended jobs before termination."
while IFS= read -r pid; do
    kill -CONT "$pid" 2>/dev/null || true
done < <(printf '%s\n' "${!PID_REASONS[@]}" | sort -n)

log "Sending TERM."
while IFS= read -r pid; do
    kill -TERM "$pid" 2>/dev/null || true
done < <(printf '%s\n' "${!PID_REASONS[@]}" | sort -n)

deadline=$((SECONDS + 3))
remaining=()
while (( SECONDS < deadline )); do
    remaining=()
    while IFS= read -r pid; do
        if kill -0 "$pid" 2>/dev/null; then
            remaining+=("$pid")
        fi
    done < <(printf '%s\n' "${!PID_REASONS[@]}" | sort -n)

    if [[ "${#remaining[@]}" -eq 0 ]]; then
        break
    fi

    sleep 0.2
done

if [[ "${#remaining[@]}" -gt 0 ]]; then
    log "Sending KILL to remaining processes: ${remaining[*]}"
    for pid in "${remaining[@]}"; do
        kill -KILL "$pid" 2>/dev/null || true
    done
fi

leftovers=()
while IFS= read -r pid; do
    if kill -0 "$pid" 2>/dev/null; then
        leftovers+=("$pid")
    fi
done < <(printf '%s\n' "${!PID_REASONS[@]}" | sort -n)

if [[ "${#leftovers[@]}" -gt 0 ]]; then
    die "Some processes are still alive after KILL: ${leftovers[*]}"
fi

log "Done. Matching local ZMQ processes were terminated."
