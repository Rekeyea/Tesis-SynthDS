#!/usr/bin/env python3
import json
import pandas as pd
import numpy as np
import argparse
from datetime import datetime, timedelta
import time
import os
import glob

# Define baseline vital values for each patient category
PATIENT_CATEGORY_BASELINES = {
    "HEALTHY": {
        "RESPIRATORY_RATE": 16,
        "OXYGEN_SATURATION": 98,
        "BLOOD_PRESSURE_SYSTOLIC": 120,
        "HEART_RATE": 70,
        "TEMPERATURE": 36.8,
        "CONSCIOUSNESS": 1
    },
    "ILL_STABLE": {
        "RESPIRATORY_RATE": 20,
        "OXYGEN_SATURATION": 95,
        "BLOOD_PRESSURE_SYSTOLIC": 110,
        "HEART_RATE": 80,
        "TEMPERATURE": 37.5,
        "CONSCIOUSNESS": 1
    },
    "HEALTHY_DETERIORATING": {
        "RESPIRATORY_RATE": 18,
        "OXYGEN_SATURATION": 97,
        "BLOOD_PRESSURE_SYSTOLIC": 115,
        "HEART_RATE": 75,
        "TEMPERATURE": 37.0,
        "CONSCIOUSNESS": 1
    }
}

# For HEALTHY_DETERIORATING patients, these factors will be applied linearly over time
DETERIORATION_FACTORS = {
    "RESPIRATORY_RATE": 2,    # increase by 2 over the period
    "OXYGEN_SATURATION": -3,  # drop by 3
    "BLOOD_PRESSURE_SYSTOLIC": -5,
    "HEART_RATE": 5,
    "TEMPERATURE": 0.5,
    "CONSCIOUSNESS": 0
}

# Standard deviation for random noise for each vital
NOISE_STD = {
    "RESPIRATORY_RATE": 1,
    "OXYGEN_SATURATION": 1,
    "BLOOD_PRESSURE_SYSTOLIC": 3,
    "HEART_RATE": 2,
    "TEMPERATURE": 0.2,
    "CONSCIOUSNESS": 0  # assuming this remains constant (e.g., 1 for alert)
}

def parse_time_range(time_range_str):
    """Convert a time range string into a timedelta."""
    time_ranges = {
        "1_hour": timedelta(hours=1),
        "1_day": timedelta(days=1),
        "1_week": timedelta(weeks=1),
        "1_month": timedelta(days=30),  # approximate a month as 30 days
        "1_year": timedelta(days=365)
    }
    
    if time_range_str not in time_ranges:
        raise ValueError(f"Unsupported time range: {time_range_str}")
    
    return time_ranges[time_range_str]

def load_config(config_file):
    """Load and return the JSON configuration."""
    with open(config_file, "r") as f:
        return json.load(f)

def generate_timestamp_series(start_time, end_time, measurement_rate, jitter=20):
    """Generate a series of timestamps with jitter."""
    # Calculate number of points needed
    duration_seconds = (end_time - start_time).total_seconds()
    num_points = int(duration_seconds / measurement_rate) + 1
    
    # Create regular time points
    regular_times = [start_time + timedelta(seconds=i * measurement_rate) for i in range(num_points)]
    
    # Add jitter to each timestamp
    jittered_times = [t + timedelta(seconds=np.random.uniform(-jitter, jitter)) for t in regular_times]
    
    # Filter timestamps that may have gone beyond end_time due to jitter
    return [t for t in jittered_times if t <= end_time]

def ensure_temp_dir(base_dir="temp_data"):
    """Ensure the temporary directory exists and is empty."""
    if os.path.exists(base_dir):
        # Clean up any existing temporary files
        for f in glob.glob(os.path.join(base_dir, "*.csv")):
            os.remove(f)
    else:
        os.makedirs(base_dir)
    return base_dir

def generate_device_data(device, patients, start_time, end_time, total_secs, temp_dir):
    """Generate data for a single device and write to a temporary file."""
    device_id = device["device_id"]
    patient_ids = device["patient_ids"]
    
    # Battery parameters with defaults
    battery_conf = device.get("battery", {"initial": 100, "drain_rate": 0.01})
    battery_level = battery_conf.get("initial", 100)
    drain_rate = battery_conf.get("drain_rate", 0.01)
    
    # Signal strength parameters with defaults
    signal_conf = device.get("signal_strength", {"base": 0.7, "variation": 0.15})
    signal_base = signal_conf.get("base", 0.7)
    signal_variation = signal_conf.get("variation", 0.15)
    
    # Each device configuration lists the vitals it measures and the measurement rate (in seconds)
    vitals_conf = device.get("vitals", {})
    
    device_data = []
    
    for vital, vital_params in vitals_conf.items():
        measurement_rate = vital_params.get("measurement_rate", 60)
        
        # Generate timestamps for this vital
        timestamps = generate_timestamp_series(start_time, end_time, measurement_rate)
        
        # Process all patients for this device
        for patient_id in patient_ids:
            patient = patients.get(patient_id)
            if not patient:
                print(f"Warning: Device {device_id} references unknown patient {patient_id}.")
                continue
            
            category = patient["category"]
            baseline = PATIENT_CATEGORY_BASELINES.get(category)
            if baseline is None:
                print(f"Warning: No baseline defined for category {category} on device {device_id}.")
                continue
            
            # Combine device_id and patient_id in the format {device}_{patient}
            combined_device_id = f"{device_id}_{patient_id}"
            
            # Pre-calculate values in bulk using NumPy arrays
            n_measurements = len(timestamps)
            
            # Create arrays for each property
            base_values = np.full(n_measurements, baseline[vital])
            
            # Add noise
            noise = np.random.normal(0, NOISE_STD.get(vital, 1), n_measurements)
            
            # Calculate time fractions for deterioration
            elapsed_secs = np.array([(t - start_time).total_seconds() for t in timestamps])
            time_fractions = elapsed_secs / total_secs if total_secs > 0 else np.zeros(n_measurements)
            
            # Apply deterioration
            deterioration = np.zeros(n_measurements)
            if category == "HEALTHY_DETERIORATING":
                deterioration = DETERIORATION_FACTORS.get(vital, 0) * time_fractions
            
            # Calculate final values
            values = base_values + noise + deterioration
            
            # Battery calculations
            measurement_counts = np.arange(n_measurements)
            battery_drain = drain_rate * measurement_counts
            battery_noise = np.random.normal(0, 0.5, n_measurements)
            battery_levels = np.clip(battery_level - battery_drain + battery_noise, 0, 100)
            
            # Signal strength
            signal_strengths = np.clip(signal_base + np.random.uniform(-signal_variation, signal_variation, n_measurements), 0, 1)
            
            # Delays
            delays = np.random.uniform(0, 5, n_measurements)
            
            # Create a batch of records
            batch_data = []
            for i in range(n_measurements):
                # Calculate seconds from start time instead of using ISO format
                seconds_from_start = round(elapsed_secs[i], 2)
                
                batch_data.append({
                    "device_id": combined_device_id,
                    "measurement_type": vital,
                    "timestamp": seconds_from_start,  # Changed from ISO format to seconds from start
                    "raw_value": round(float(values[i]), 2),
                    "battery": round(float(battery_levels[i]), 2),
                    "signal_strength": round(float(signal_strengths[i]), 2),
                    "delay": round(float(delays[i]), 2)
                })
            
            # Append the batch data to device data
            device_data.extend(batch_data)
            
            # If we've accumulated a lot of data, write to disk and clear
            if len(device_data) > 1000000:  # 1 million records
                df = pd.DataFrame(device_data)
                temp_file = os.path.join(temp_dir, f"{device_id}_{len(os.listdir(temp_dir))}.csv")
                df.to_csv(temp_file, index=False)
                device_data = []
    
    # Write any remaining data
    if device_data:
        df = pd.DataFrame(device_data)
        temp_file = os.path.join(temp_dir, f"{device_id}_{len(os.listdir(temp_dir))}.csv")
        df.to_csv(temp_file, index=False)
    
    return len(device_data)

def generate_measurements(config, temp_dir):
    """
    Generate synthetic measurement records for each device and vital using pandas.
    Writes data to temporary files and returns the number of records generated.
    """
    print("Starting data generation...")
    start_execution = time.time()
    
    # Define the time range
    time_range = parse_time_range(config["time_range"])
    end_time = datetime.now()
    start_time = end_time - time_range
    total_secs = time_range.total_seconds()
    
    # Build a quick lookup for patients using their patient_id
    patients = {p["patient_id"]: p for p in config["patients"]}
    
    device_count = len(config["devices"])
    current_device = 0
    total_records = 0
    
    for device in config["devices"]:
        current_device += 1
        print(f"Processing device {current_device}/{device_count}: {device['device_id']}")
        
        records = generate_device_data(
            device, patients, start_time, end_time, total_secs, temp_dir
        )
        total_records += records
    
    print(f"Generated approximately {total_records} records in {time.time() - start_execution:.2f} seconds")
    return total_records

def merge_temp_files(temp_dir, output_file, chunk_size=100000):
    """Merge all temporary CSV files into a single output file."""
    start_time = time.time()
    print(f"Merging temporary files to {output_file}...")
    
    # Get all temporary files
    temp_files = glob.glob(os.path.join(temp_dir, "*.csv"))
    if not temp_files:
        print("No temporary files found to merge!")
        return
    
    # Write header from the first file
    header = pd.read_csv(temp_files[0], nrows=0).columns.tolist()
    
    # Open the output file in write mode to clear it if it exists
    with open(output_file, 'w', newline='') as f_out:
        # Write the header
        f_out.write(','.join(header) + '\n')
        
        # Process each temp file
        for i, temp_file in enumerate(temp_files):
            print(f"Processing file {i+1}/{len(temp_files)}: {os.path.basename(temp_file)}")
            
            # Process in chunks to avoid memory issues
            for chunk in pd.read_csv(temp_file, chunksize=chunk_size):
                chunk.to_csv(f_out, header=False, index=False, mode='a')
    
    print(f"Merged all files in {time.time() - start_time:.2f} seconds")

def main():
    parser = argparse.ArgumentParser(
        description="Generate synthetic patient monitoring data using pandas for performance."
    )
    parser.add_argument("config_file", help="Path to the JSON configuration file")
    parser.add_argument("output_file", help="Path to the output CSV file")
    parser.add_argument("--temp-dir", default="temp_data", help="Directory for temporary files")
    parser.add_argument("--keep-temp", action="store_true", help="Keep temporary files after processing")
    args = parser.parse_args()

    print(f"Reading configuration from {args.config_file}")
    config = load_config(args.config_file)
    
    # Prepare temporary directory
    temp_dir = ensure_temp_dir(args.temp_dir)
    
    # Generate data to temporary files
    generate_measurements(config, temp_dir)
    
    # Merge temporary files into final output
    merge_temp_files(temp_dir, args.output_file)
    
    # Clean up temporary files if not keeping them
    if not args.keep_temp:
        print("Cleaning up temporary files...")
        for f in glob.glob(os.path.join(temp_dir, "*.csv")):
            os.remove(f)
        os.rmdir(temp_dir)
    
    print(f"Data generation complete. Output written to {args.output_file}")

if __name__ == "__main__":
    main()