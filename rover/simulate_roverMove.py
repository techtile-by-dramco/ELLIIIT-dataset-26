from rover import run_rover, load_config, update_config


def main():
    example_speaker_coords = [200, 200, 0.215]
    
    zmq_params = {
        "speaker_coordinates": example_speaker_coords,
        "feed_rate": 100
    }

    print("Loading base config...")
    base_config = load_config()

    print("Updating config with test parameters...")
    for key, value in zmq_params.items():
        update_config(key, value)

    config = load_config()

    print("Running acoustic measurement...")

    result = run_rover(base_config, config)
    
    print("\n--- Measurement Complete ---")
    print(f"X: {result['x']}")
    print(f"Y: {result['y']}")
    print(f"Duration:         {result['duration_s']} seconds")

if __name__ == "__main__":
    main()