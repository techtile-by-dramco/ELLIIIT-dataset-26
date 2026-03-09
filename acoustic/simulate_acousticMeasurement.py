from acousticMeasurement import run_acoustic_measurement, load_config, update_config

def main():
    example_speaker_coords = [7.38, 0.01, 1.07]
    
    # Define the parameters you want to override for this test
    zmq_params = {
        "speaker_coordinates": example_speaker_coords,
        "chirp_f_start":   20000,
        "chirp_f_stop":    30000,
        "chirp_duration":  0.10,
        "chirp_DC":        0.1,
        "chirp_ampl":      0.05,
        "plot_signals":    True,  # Note: Capitalized 'True'
        "get_system_info": True,  # Note: Capitalized 'True'
    }

    print("Loading base config...")
    base_config = load_config()

    print("Updating config with test parameters...")
    for key, value in zmq_params.items():
        update_config(key, value)

    # Load the freshly updated config
    config = load_config()

    print("Running acoustic measurement...")
    # Run the measurement (plots will pop up automatically during this step)
    result = run_acoustic_measurement(base_config, config)
    
    print("\n--- Measurement Complete ---")
    print(f"Results saved to: {result['csv_file']}")
    print(f"Number of mics:   {result['n_mics']}")
    print(f"Duration:         {result['duration_s']} seconds")

if __name__ == "__main__":
    main()