import os
import sys
import yaml
import argparse
import config
import time

def ask_yes_no(prompt, default=True):
    """
    Ask a yes/no question via input() and return True/False.
    
    Parameters:
        prompt (str): The question to show.
        default (bool): Default choice if user presses Enter. True=yes, False=no.
    
    Returns:
        bool: True for yes, False for no.
    """
    if default:
        prompt_str = f"{prompt} [Y/n]: "
    else:
        prompt_str = f"{prompt} [y/N]: "

    while True:
        answer = input(prompt_str).strip().lower()
        if not answer:  # Enter pressed
            return default
        if answer in ("y", "yes"):
            return True
        if answer in ("n", "no"):
            return False
        print("Please enter 'y(es)' or 'n(o)'.")
        

parser = argparse.ArgumentParser(
    description="Control power of the tiles. CAREFUL! This can power up/down everything on the tile(s). If no arguments are given, it will only show the power status of the hosts."
)

parser.add_argument(
    "--power-up", "-u",
    action="store_true",
    help="Power-up a tile."
)

parser.add_argument(
    "--power-down", "-d",
    action="store_true",
    help="Power-down a tile."
)

parser.add_argument(
    "--tiles", "-t",
    help="Optional list of tiles."
)

args = parser.parse_args()

# We start by setting some paths
settings_path = os.path.join(config.PROJECT_DIR, "experiment-settings.yaml")

# Check if the tile-management repo is in the default location (no use in continuing if it's not)
if not config.check_tile_management_repo():
    sys.exit(config.ERRORS["REPO_ERROR"])

# Import code from the tile-management repo
sys.path.append(config.UTILS_DIR)
from ansible_utils import get_target_hosts
from midspan_utils import midspan_support_class

# Output some general information before we start
print("Experiment project directory: ", config.PROJECT_DIR) # should point to tile-management repo clone

# Read experiment settings
with open(settings_path, "r") as f:
    experiment_settings = yaml.safe_load(f)

if args.tiles:
    tiles = args.tiles
else:
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

snmp_user = os.getenv("SNMP_USER")
snmp_password = os.getenv("SNMP_PASSWORD")

if snmp_user is None:
    raise RuntimeError("SNMP_USER environment variable is not set")
if snmp_password is None:
    raise RuntimeError("SNMP_PASSWORD environment variable is not set")

midspan = midspan_support_class(snmp_user, snmp_password)

if args.power_down:
    if not ask_yes_no("Powering down tiles, are you sure you want to continue?"):
        print("Power-down aborted.")
        quit()
    else:
        midspan.setPortOnOff(host_list, midspan_support_class.OFF)

if args.power_up:
    if not ask_yes_no("Powering up tiles, are you sure you want to continue?"):
        print("Power-up aborted.")
        quit()
    else:
        midspan.setPortOnOff(host_list, midspan_support_class.ON)

if len(host_list) > 0:
    print("┌───────┬────────┬─────────┬───────────┬───────────┐")
    print("| host  | on/off | power   | max. pow. | poe class |")
    print("├───────┼────────┼─────────┼───────────┼───────────┤")
for host in host_list:
    (onOff, portPower, portMaxPower, poeClass) = midspan.getPortStatus(host)
    sstr = ""
    if onOff == 1:
        sstr = "on "
    else:
        sstr = "off"
    print(f"| {host:s}   | {sstr:s}    | {portPower:<7d} | {portMaxPower:<9d} | {poeClass:<9d} |")
if len(host_list) > 0:
    print("└───────┴────────┴─────────┴───────────┴───────────┘")
    