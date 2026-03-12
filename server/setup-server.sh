#!/bin/bash
# virtual environment activation script
# Run with: path-to/setup-server.sh

set -e

INSTALL_PACKAGES=1                      # can be put to 0 to speed up activation, once all necessary packages are installed
CLONE_TILE_MANAGEMENT_REPO=1            # 0 while developing the tile-management repo, 1 otherwise
TILE_MANAGEMENT_REPO_PARENT="$HOME"     # don't change, other locations not supported

# Absolute path to the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [[ "${CLONE_TILE_MANAGEMENT_REPO:-0}" == "1" ]]; then
    echo "Cloning tile-management repo."

    REPO_URL="https://github.com/techtile-by-dramco/tile-management.git"
    LOCAL_DIR="$HOME/tile-management"

    if [[ -d "$LOCAL_DIR/.git" ]]; then
        echo "Repository exists. Fetching latest changes..."
        cd "$LOCAL_DIR" || exit 1
        git fetch --all
        git pull origin main   # adjust branch if needed
        cd - > /dev/null
    else
        echo "Repository does not exist. Cloning..."
        git clone "$REPO_URL" "$LOCAL_DIR"
    fi
fi

# Virtual environment directory NEXT TO the script
VENV_DIR="${SCRIPT_DIR}"

# Detect if a virtual environment is already active
if [[ -n "$VIRTUAL_ENV" ]]; then
    echo "Warning: A virtual environment is already active: $VIRTUAL_ENV"
    echo "Skipping creation/activation to avoid nested venvs."
    return 0 2>/dev/null || exit 0
fi

# Create venv if it doesn't exist
if [[ ! -f "$VENV_DIR/bin/activate" ]]; then
    echo "Creating virtual environment in $VENV_DIR..."
    python3 -m venv "$VENV_DIR"
    echo "Virtual environment created."
fi

# Activate the venv
# shellcheck disable=SC1091
source "$VENV_DIR/bin/activate"

# Optional: ensure PATH is correct
export PATH="$VENV_DIR/bin:$PATH"
unset PYTHONHOME

echo "Virtual environment is now active."
echo "Python: $(python --version)"
echo "pip: $(pip --version)"

# === Package installation ===
# Only install packages if a special flag is set (to avoid crashes on login)
if [[ "${INSTALL_PACKAGES:-0}" == "1" ]]; then
    echo "Installing required packages..."
    pip install --upgrade --quiet pip
    pip install --upgrade --quiet ansible-runner ansible-core pyzmq pyvisa numpy scipy
    # add other python packages you might need
fi

echo "Before running python scripts, make sure to active your venv:"
echo "  => source bin/activate"
