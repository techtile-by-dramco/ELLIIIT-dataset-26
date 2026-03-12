import os
import sys
import yaml

# We start by setting some paths
_script_dir = os.path.dirname(os.path.abspath(__file__))
_project_dir = os.path.dirname(_script_dir)
_home = os.environ["HOME"]
_tile_management_base_dir = os.path.join(_home, "tile-management")
_tile_management_server_dir = os.path.join(_tile_management_base_dir, "server")
_tile_management_playbook_dir = os.path.join(_tile_management_base_dir, "playbooks")
_tile_management_inventory_dir = os.path.join(_tile_management_base_dir, "inventory")
_inventory_path = os.path.join(_tile_management_inventory_dir, "hosts.yaml")

# Define "public" configs
PROJECT_DIR = _project_dir
UTILS_DIR = _tile_management_server_dir
PLAYBOOK_DIR = _tile_management_playbook_dir
INVENTORY_PATH = _inventory_path
TILE_MANAGEMENT_REPO_ORG = "techtile-by-dramco"
TILE_MANAGEMENT_REPO_NAME = "tile-management"
TILE_MANAGEMENT_REPO_DIR = "/home/pi/tile-management"

def check_tile_management_repo():
    # We look for the tile-management repo
    if not os.path.isdir(_tile_management_base_dir):
        print("Could not find tile-management repository at the default location (", _tile_management_base_dir, ")")
        print("Did you run ./setup-server.sh with CLONE_TILE_MANAGEMENT_REPO=1 ?")
        return False
    return True

ERRORS = {
    "REPO_ERROR": -1,
    "NO_TILES_ERROR": -2,
    "CONNECTIVITY_ERROR": -3,
    "ARGUMENT_ERROR": -4
}
