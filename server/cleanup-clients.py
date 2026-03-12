import os
import sys
import yaml
import argparse
import config

parser = argparse.ArgumentParser(
    description="Cleanup the home directory of the tiles' raspberry pi's."
)

parser.add_argument(
    "--ansible-output", "-a",
    action="store_true",
    help="Enable ansible output"
)

args = parser.parse_args()

# We start by setting some paths
settings_path = os.path.join(config.PROJECT_DIR, "experiment-settings.yaml")

# Check if the tile-management repo is in the default location (no use in continuing if it's not)
if not config.check_tile_management_repo():
    sys.exit(config.ERRORS["REPO_ERROR"])

# Import code from the tile-management repo
sys.path.append(config.UTILS_DIR)
from ansible_utils import get_target_hosts, run_playbook

# Output some general information before we start
print("Experiment project directory: ", config.PROJECT_DIR) # should point to tile-management repo clone

# Read experiment settings
with open(settings_path, "r") as f:
    experiment_settings = yaml.safe_load(f)

tiles = experiment_settings.get("tiles", "")
if len(tiles) == 0:
    print("The experiment doesn't target any tiles.")
    sys.exit(config.ERRORS["NO_TILES_ERROR"])
test_connectivity = experiment_settings.get("test_connectivity", True)
halt_on_connectivity_failure = experiment_settings.get("halt_on_connectivity_failure", True)

# host list can be used to identify individual tiles from group names
# We don't need it to run ansible playbooks, but it is a first check to see if the tiles are specified correctly
host_list = get_target_hosts(config.INVENTORY_PATH, limit=tiles, suppress_warnings=True)
print("Working on", len(host_list) ,"tile(s):", tiles)

# First we test connectivity
nr_active_tiles = 0
if test_connectivity:
    print("Testing connectivity ... ")
    playbook_path = os.path.join(config.PLAYBOOK_DIR, "ping.yaml")

    (nr_active_tiles, tiles, failed_tiles) = run_playbook(
        config.PROJECT_DIR,
        playbook_path,
        config.INVENTORY_PATH,
        extra_vars=None,
        hosts=tiles,
        mute_output=not(args.ansible_output),
        suppress_warnings=True,
        cleanup=True
    )

    if not (nr_active_tiles == len(host_list)):
        print("Unable to connect to all tiles.")
        print("Inactive tiles:", failed_tiles)
        if halt_on_connectivity_failure:
            print("Aborting (halt_on_connectivity_failure = True)")
            sys.exit(config.ERRORS["CONNECTIVITY_ERROR"])
        else:
            print("Proceeding with", nr_active_tiles, "tiles(s):", tiles)
else:
    # we did not test connectivity so we assume all tiles are active
    nr_active_tiles = len(host_list)
                
prev_nr_active_tiles = nr_active_tiles

print("Disabling experiment-launcher.service ... ")
playbook_path = os.path.join(config.PLAYBOOK_DIR, "run-script.yaml")

(nr_active_tiles, tiles, failed_tiles) = run_playbook(
    config.PROJECT_DIR,
    playbook_path,
    config.INVENTORY_PATH,
    extra_vars={
        'script_path': os.path.join(config.TILE_MANAGEMENT_REPO_DIR, 'tiles/install-experiment.sh'),
        'sudo': 'yes',
        'script_args': 'remove'
    },
    hosts=tiles,
    mute_output=not(args.ansible_output),
    suppress_warnings=True,
    cleanup=True
)

if not (nr_active_tiles == len(host_list)):
    print("Unable to connect to all tiles.")
    print("Inactive tiles:", failed_tiles)
    if halt_on_connectivity_failure:
        print("Aborting (halt_on_connectivity_failure = True)")
        sys.exit(config.ERRORS["CONNECTIVITY_ERROR"])
    else:
        print("Proceeding with", nr_active_tiles, "tiles(s):", tiles)
        
prev_nr_active_tiles = nr_active_tiles

print("Disabled experiment-launcher.service on tiles(s):", tiles)       

print("Cleaning tile home-directory ... ")
playbook_path = os.path.join(config.PLAYBOOK_DIR, "clean-home.yaml")

(nr_active_tiles, tiles, failed_tiles) = run_playbook(
    config.PROJECT_DIR,
    playbook_path,
    config.INVENTORY_PATH,
    extra_vars=None,
    hosts=tiles,
    mute_output=not(args.ansible_output),
    suppress_warnings=True,
    cleanup=True
)

if not (nr_active_tiles == len(host_list)):
    print("Unable to connect to all tiles.")
    print("Inactive tiles:", failed_tiles)
    if halt_on_connectivity_failure:
        print("Aborting (halt_on_connectivity_failure = True)")
        sys.exit(config.ERRORS["CONNECTIVITY_ERROR"])
    else:
        print("Proceeding with", nr_active_tiles, "tiles(s):", tiles)

print("Cleaned the home directory of tiles(s):", tiles)
        
print("Done.")