import os
import csv
import json
import time
import asyncio
import multiprocessing
import argparse
from typing import List, Dict, Any
from kafka import KafkaProducer
from datetime import datetime, timedelta

# Configuration
KAFKA_BOOTSTRAP_SERVERS = ["localhost:9091","localhost:9092","localhost:9093"]  # Update with your Kafka broker addresses
KAFKA_TOPIC = 'raw.measurements'  # Update with your desired topic
PATIENT_DATA_DIR = 'patient_data'  # Directory containing patient CSV files

# Time offset options
TIME_OFFSETS = {
    "hour": timedelta(hours=1),
    "day": timedelta(days=1),
    "week": timedelta(weeks=1),
    "month": timedelta(days=30),  # Approximate
    "year": timedelta(days=365)   # Approximate
}

# Initialize Kafka producer with JSON serialization
def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: str(k).encode('utf-8')
    )

# Read a CSV file and return its contents as a list of dictionaries
def read_csv_file(file_path: str) -> List[Dict[str, Any]]:
    rows = []
    with open(file_path, 'r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            # Convert numeric fields to appropriate types
            for field in ['timestamp', 'raw_value', 'battery', 'signal_strength', 'delay']:
                if field in row and row[field]:
                    row[field] = float(row[field])
            rows.append(row)
    return rows

# Send a single row to Kafka with the specified delay
async def send_row_with_delay(producer, row, delay, time_offset):
    if delay > 0:
        await asyncio.sleep(delay)
    
    # Use device_id as the message key for partitioning
    key = row.get('device_id', '')
    
    # Add metadata
    row['processed_at'] = datetime.now().isoformat()
    
    # Rename timestamp to measurement_timestamp and adjust it
    if 'timestamp' in row:
        # Set the base date (now minus the specified offset)
        base_date = datetime.now() - time_offset
        
        # Add the timestamp value (in seconds) to the base date
        seconds_value = row['timestamp']
        measurement_date = base_date + timedelta(seconds=seconds_value)
        
        # Format as ISO 8601 date string
        row['measurement_timestamp'] = measurement_date.isoformat()
        
        # Remove the original timestamp field
        del row['timestamp']
    
    # Send to Kafka
    producer.send(KAFKA_TOPIC, key=key, value=row)
    producer.flush()
    print(f"Sent row: {row['device_id']} - {row.get('measurement_timestamp')} (delayed by {delay}s)")

# Process a single file in a completely separate process
def process_file_worker(file_path: str, time_offset_name: str):
    """This function runs in a separate process and handles processing a single file"""
    try:
        print(f"Processing file: {file_path} with time offset: {time_offset_name}")
        
        # Get the time offset
        time_offset = TIME_OFFSETS.get(time_offset_name, TIME_OFFSETS["hour"])
        
        # Read all rows from the CSV file
        rows = read_csv_file(file_path)
        
        # Sort rows by timestamp for correct ordering
        rows.sort(key=lambda x: x['timestamp'])
        
        # Create a producer for this process
        producer = create_producer()
        
        # Create an event loop for this process
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        # Create tasks for each row
        tasks = []
        for row in rows:
            delay = row.get('delay', 0)
            task = loop.create_task(send_row_with_delay(producer, row, delay, time_offset))
            tasks.append(task)
        
        # Wait for all tasks to complete
        loop.run_until_complete(asyncio.gather(*tasks))
        loop.close()
        
        # Close the producer
        producer.close()
        print(f"Completed processing file: {file_path}")
        
    except Exception as e:
        print(f"Error processing file {file_path}: {str(e)}")

# Main function to discover files and start processing
def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Process patient data files and send to Kafka')
    parser.add_argument('--time-offset', choices=TIME_OFFSETS.keys(), default='hour',
                        help='Time offset for measurement timestamps (hour, day, week, month, year)')
    args = parser.parse_args()
    
    # Get all CSV files in the patient_data directory
    csv_files = [
        os.path.join(PATIENT_DATA_DIR, f) 
        for f in os.listdir(PATIENT_DATA_DIR) 
        if f.endswith('.csv')
    ]
    
    if not csv_files:
        print(f"No CSV files found in {PATIENT_DATA_DIR}")
        return
    
    print(f"Found {len(csv_files)} CSV files to process with time offset: {args.time_offset}")
    
    # Use multiprocessing to handle each file in a separate process
    processes = []
    for file_path in csv_files:
        p = multiprocessing.Process(target=process_file_worker, args=(file_path, args.time_offset))
        processes.append(p)
        p.start()
    
    # Wait for all processes to complete
    for p in processes:
        p.join()
    
    print("All files have been processed successfully")

# Entry point
if __name__ == "__main__":
    main()