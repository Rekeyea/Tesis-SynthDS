#!/usr/bin/env python3
import json
import csv
import time
import argparse
import logging

from datetime import datetime, timedelta
from kafka import KafkaProducer
import sys
import os

# Set up logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Stream CSV data to Kafka with time-based simulation')
    parser.add_argument('--csv-file', type=str, default='sorted.csv', help='Path to the CSV file')
    parser.add_argument('--config-file', type=str, default='config.json', help='Path to the config file')
    parser.add_argument('--rate', type=float, default=100, 
                        help='Number of rows to send per second')
    return parser.parse_args()

def load_config(config_file):
    """Load configuration from JSON file."""
    try:
        with open(config_file, 'r') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Failed to load config file: {e}")
        sys.exit(1)

def calculate_start_time(time_range):
    """Calculate the start time based on the time_range from config."""
    now = datetime.now()
    
    if time_range == "1_hour":
        return now - timedelta(hours=1)
    elif time_range == "1_day":
        return now - timedelta(days=1)
    elif time_range == "1_week":
        return now - timedelta(weeks=1)
    elif time_range == "1_month":
        return now - timedelta(days=30)  # Approximation
    elif time_range == "1_year":
        return now - timedelta(days=365)  # Approximation
    else:
        logger.error(f"Invalid time_range: {time_range}")
        sys.exit(1)

def create_kafka_producer():
    """Create a Kafka producer."""
    try:
        return KafkaProducer(
            bootstrap_servers=["localhost:9091", "localhost:9092", "localhost:9093"],
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
    except Exception as e:
        logger.error(f"Failed to create Kafka producer: {e}")
        sys.exit(1)

def send_to_kafka(producer, data):
    """Send data to Kafka topic."""
    try:
        data["measurement_timestamp"] = data["timestamp"]
        future = producer.send("raw.measurements", value=data)
        # Block for 'synchronous' sends
        record_metadata = future.get(timeout=10)
        logger.debug(f"Sent to partition {record_metadata.partition}, offset {record_metadata.offset}")
        return True
    except Exception as e:
        logger.error(f"Failed to send to Kafka: {e}")
        return False

def count_total_rows(csv_file):
    """Count total rows in CSV file for progress reporting."""
    try:
        with open(csv_file, 'r') as f:
            # Skip header
            next(f)
            return sum(1 for _ in f)
    except Exception as e:
        logger.error(f"Error counting rows: {e}")
        return None

# Function removed as CSV is always sorted

def stream_sorted_csv(csv_file, producer, start_time, rows_per_second):
    """Stream CSV file that is already sorted by timestamp."""
    try:
        total_rows = count_total_rows(csv_file)
        start_process_time = time.time()
        rows_sent = 0
        
        # Calculate wait time between sending rows
        wait_time = 1 / rows_per_second if rows_per_second > 0 else 0
        
        # Track performance metrics
        last_report_time = time.time()
        last_report_count = 0
        
        with open(csv_file, 'r') as f:
            reader = csv.DictReader(f)
            
            for row in reader:
                # Convert row timestamp to actual datetime
                seconds_offset = float(row['timestamp'])
                target_time = start_time + timedelta(seconds=seconds_offset)
                
                # Create message
                message = {
                    'device_id': row['device_id'],
                    'measurement_type': row['measurement_type'],
                    'timestamp': target_time.isoformat(),
                    'raw_timestamp': seconds_offset,
                    'raw_value': float(row['raw_value']),
                    'battery': float(row['battery']),
                    'signal_strength': float(row['signal_strength'])
                }
                
                # Calculate how long to wait before sending this message
                current_time = time.time()
                
                # Send message to Kafka
                send_to_kafka(producer, message)
                if wait_time > 0:
                    time.sleep(wait_time)
                
                # Update counters
                rows_sent += 1
                
                # Report progress periodically
                if rows_sent % 1000 == 0:
                    current_time = time.time()
                    elapsed = current_time - last_report_time
                    records_since_last = rows_sent - last_report_count
                    
                    # Calculate actual sending rate
                    actual_rate = records_since_last / elapsed if elapsed > 0 else 0
                    
                    # Calculate progress and ETA
                    progress = (rows_sent / total_rows * 100) if total_rows else "unknown"
                    
                    if total_rows:
                        elapsed_total = current_time - start_process_time
                        records_per_sec_avg = rows_sent / elapsed_total if elapsed_total > 0 else 0
                        remaining_records = total_rows - rows_sent
                        eta_seconds = remaining_records / records_per_sec_avg if records_per_sec_avg > 0 else 0
                        eta = str(timedelta(seconds=int(eta_seconds)))
                        
                        logger.info(f"Progress: {rows_sent:,}/{total_rows:,} rows ({progress:.1f}%) | "
                                    f"Rate: {actual_rate:.1f}/s (target: {rows_per_second}/s) | "
                                    f"ETA: {eta}")
                    else:
                        logger.info(f"Progress: {rows_sent:,} rows | "
                                    f"Rate: {actual_rate:.1f}/s (target: {rows_per_second}/s)")
                    
                    # Reset counters for next report
                    last_report_time = current_time
                    last_report_count = rows_sent
            
            # Final report
            total_time = time.time() - start_process_time
            avg_rate = rows_sent / total_time if total_time > 0 else 0
            logger.info(f"Finished sending all {rows_sent:,} messages in {total_time:.1f}s (avg rate: {avg_rate:.1f}/s)")
            
    except Exception as e:
        logger.error(f"Error processing CSV: {e}")
        sys.exit(1)

def main():
    """Main entry point of the script."""
    args = parse_arguments()
    
    # Validate that files exist
    if not os.path.exists(args.csv_file):
        logger.error(f"CSV file not found: {args.csv_file}")
        sys.exit(1)
    
    if not os.path.exists(args.config_file):
        logger.error(f"Config file not found: {args.config_file}")
        sys.exit(1)
    
    # Load configuration
    config = load_config(args.config_file)
    
    if 'time_range' not in config:
        logger.error("Config file missing 'time_range' property")
        sys.exit(1)
    
    # Calculate start time based on config
    start_time = calculate_start_time(config['time_range'])
    logger.info(f"Using start time: {start_time} (based on time_range: {config['time_range']})")
    
    # Create Kafka producer
    producer = create_kafka_producer()
    
    # Stream the sorted CSV to Kafka
    logger.info(f"Processing pre-sorted CSV at {args.rate} rows per second")
    stream_sorted_csv(args.csv_file, producer, start_time, args.rate)
    
    # Flush and close the producer
    producer.flush()
    producer.close()
    
    logger.info("Script completed successfully")

if __name__ == "__main__":
    main()