from rover import run_rover
import json
from pathlib import Path

BASE_DIR    = Path(__file__).parent.resolve()
CONFIG_PATH = BASE_DIR / "config.json"


def load_config(path: Path = CONFIG_PATH) -> dict:
    if not path.exists():
        raise FileNotFoundError(f"Rover config not found: {path}")
    with open(path, "r") as f:
        config = json.load(f)
    _validate_config(config)
    return config


def _validate_config(config: dict) -> None:
    required = ["serial_port", "feed_rate"]
    missing = [k for k in required if k not in config]
    if missing:
        raise ValueError(f"Rover config is missing keys: {missing}")
    if config["feed_rate"] <= 0:
        raise ValueError("feed_rate must be > 0")
    
def main():
    base_config = load_config()

    print("Running acoustic measurement...")

    run_rover(100,200,base_config)
    
    print("\n--- Measurement Complete ---")

if __name__ == "__main__":
    main()
