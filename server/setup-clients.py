import os
import sys
import yaml
import argparse
import config

parser = argparse.ArgumentParser(
    description="""
Setup the tiles' raspberry pi's so they can run the experiment.

This involves:
    - making sure all installed packages are up-to-date
    - installing required packages
    - pulling both the tile-management and the experiment repo
    - downloading the b210 firmware images
    - testing if the UHD python API works
""",
    formatter_class=argparse.RawDescriptionHelpFormatter,
)

parser.add_argument(
    "--ansible-output", "-a",
    action="store_true",
    help="Enable ansible output"
)

parser.add_argument(
    "--skip-apt", "-s",
    action="store_true",
    help="Skip apt update/upgrade and apt install <extra-packages> (defined in experiment-settings.yaml)"
)

parser.add_argument(
    "--install-only", "-i",
    action="store_true",
    help="Run apt update/upgrade and apt install <extra-packages> (defined in experiment-settings.yaml)"
)

parser.add_argument(
    "--repos-only", "-r",
    action="store_true",
    help="Only pull the required repositories"
)

parser.add_argument(
    "--check-uhd-only", "-c",
    action="store_true",
    help="Only check if the UHD python API is available"
)

args = parser.parse_args()

if args.skip_apt and args.install_only:
    print("Conflicting arguments: --skip-apt & --install-only")
    parser.print_help()
    sys.exit(config.ERRORS["ARGUMENT_ERROR"])

if args.repos_only and args.install_only:
    print("Conflicting arguments: --repos-only & --install-only")
    parser.print_help()
    sys.exit(config.ERRORS["ARGUMENT_ERROR"])
    
if args.repos_only and args.check_uhd_only:
    print("Conflicting arguments: --repos-only & --install-only")
    parser.print_help()
    sys.exit(config.ERRORS["ARGUMENT_ERROR"])
    
if args.check_uhd_only and args.install_only:
    print("Conflicting arguments: --repos-only & --install-only")
    parser.print_help()
    sys.exit(config.ERRORS["ARGUMENT_ERROR"])

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
extra_packages = experiment_settings.get("extra_packages", "")
experiment_repo = experiment_settings.get("experiment_repo", "")
organisation = experiment_settings.get("organisation", "")

# host list can be used to identify individual tiles from group names
# We don't need it to run ansible playbooks, but it is a first check to see if the tiles are specified correctly
host_list = get_target_hosts(config.INVENTORY_PATH, limit=tiles, suppress_warnings=True)

# reassign tiles, wrongly specified tiles have been removed from list
tiles = " ".join(host_list)
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

if not (args.skip_apt or args.repos_only or args.check_uhd_only):
    print("Running apt update/upgrade ... ")
    playbook_path = os.path.join(config.PLAYBOOK_DIR, "update-upgrade.yaml")

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

    print("Updated packages on tiles(s):", tiles)
    prev_nr_active_tiles = nr_active_tiles

    print("Installing extra packages ... ")
    playbook_path = os.path.join(config.PLAYBOOK_DIR, "install-packages.yaml")

    (nr_active_tiles, tiles, failed_tiles) = run_playbook(
        config.PROJECT_DIR,
        playbook_path,
        config.INVENTORY_PATH,
        extra_vars={
            'extra_packages': extra_packages,
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
    
    print("Updated packages on tiles(s):", tiles)
    prev_nr_active_tiles = nr_active_tiles

if (not args.install_only) and (not args.check_uhd_only):
    print("Pulling the tile-management repo ... ")
    playbook_path = os.path.join(config.PLAYBOOK_DIR, "pull-repo.yaml")
    
    (nr_active_tiles, tiles, failed_tiles) = run_playbook(
        config.PROJECT_DIR,
        playbook_path,
        config.INVENTORY_PATH,
        extra_vars={
            'org_name': config.TILE_MANAGEMENT_REPO_ORG,
            'repo_name': config.TILE_MANAGEMENT_REPO_NAME
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
    
    print("Pulled tile-managament repository on tiles(s):", tiles)
    prev_nr_active_tiles = nr_active_tiles
    
if (not args.install_only) and (not args.repos_only):
    print("Checking uhd ... ")
    playbook_path = os.path.join(config.PLAYBOOK_DIR, "run-script.yaml")
    
    (nr_active_tiles, tiles, failed_tiles) = run_playbook(
        config.PROJECT_DIR,
        playbook_path,
        config.INVENTORY_PATH,
        extra_vars={
            'script_path': os.path.join(config.TILE_MANAGEMENT_REPO_DIR, 'tiles/check-uhd.sh'),
            'sudo': 'yes',
            'sudo_flags': '-E'
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

    print("UHD python API available on tiles(s):", tiles)
    
print("Done.")
