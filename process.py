import os
import csv
import json
import time
import queue
import signal
import threading
import multiprocessing
import argparse
from typing import Dict, Any, List, Optional, Tuple
from kafka import KafkaProducer
from datetime import datetime, timedelta

# Configuration
KAFKA_BOOTSTRAP_SERVERS = ["localhost:9091", "localhost:9092", "localhost:9093"]
KAFKA_TOPIC = 'raw.measurements'
PATIENT_DATA_DIR = 'patient_data'
CONFIG_FILE = 'config.json'
BATCH_SIZE = 1  # Number of records to batch before sending to Kafka
DEFAULT_RATE_LIMIT = 1000  # Default messages per second

# Define time offset mapping
TIME_OFFSETS = {
    "1_hour": timedelta(hours=1),
    "1_day": timedelta(days=1),
    "1_week": timedelta(weeks=1),
    "1_month": timedelta(days=30),
    "1_year": timedelta(days=365)
}

worker_processes = []

# Global flag for graceful shutdown
should_exit = threading.Event()

# Global message queue for rate limiting
message_queue = multiprocessing.Queue()

# Setup signal handlers
def setup_signal_handlers():
    def handle_signal(signum, frame):
        print(f"Received signal {signum}, initiating graceful shutdown...")
        should_exit.set()
        
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

def load_config():
    try:
        with open(CONFIG_FILE, 'r') as config_file:
            config = json.load(config_file)
            time_range = config.get('time_range', '1_hour')
            rate_limit = config.get('rate_limit', DEFAULT_RATE_LIMIT)
            
            if time_range not in TIME_OFFSETS:
                print(f"Warning: Unknown time range '{time_range}' in config file. Using default '1_hour'.")
                time_range = '1_hour'
            
            if not isinstance(rate_limit, (int, float)) or rate_limit <= 0:
                print(f"Warning: Invalid rate limit '{rate_limit}' in config file. Using default {DEFAULT_RATE_LIMIT} msgs/sec.")
                rate_limit = DEFAULT_RATE_LIMIT
                
            return time_range, rate_limit
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"Config issue: {str(e)}. Using defaults: time_range='1_hour', rate_limit={DEFAULT_RATE_LIMIT} msgs/sec.")
    
    return '1_hour', DEFAULT_RATE_LIMIT

def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: str(k).encode('utf-8'),
        batch_size=16384,  # Increase default batch size (in bytes)
        linger_ms=100,     # Add small delay to improve batching
        buffer_memory=67108864  # 64MB buffer
    )

class CsvFileStreamer:
    """Class to stream rows from a CSV file one at a time"""
    def __init__(self, file_path: str):
        self.file_path = file_path
        self.filename = os.path.basename(file_path)
        self.file = None
        self.reader = None
        self.rows_read = 0
        self.is_open = False
        self.is_finished = False
    
    def open(self):
        """Open the file and prepare the CSV reader"""
        if not self.is_open and not self.is_finished:
            try:
                self.file = open(self.file_path, 'r', encoding='utf-8')
                self.reader = csv.DictReader(self.file)
                self.is_open = True
                return True
            except Exception as e:
                print(f"Error opening {self.file_path}: {str(e)}")
                self.is_finished = True
                return False
        return self.is_open
    
    def get_next_row(self) -> Optional[Dict[str, Any]]:
        """Get the next row from the CSV, or None if finished"""
        if not self.is_open and not self.open():
            return None
            
        try:
            row = next(self.reader)
            self.rows_read += 1
            
            # Convert numeric fields to appropriate types
            for field in ['timestamp', 'raw_value', 'battery', 'signal_strength']:
                if field in row and row[field]:
                    try:
                        row[field] = float(row[field])
                    except (ValueError, TypeError):
                        print(f"Warning: Could not convert {field} value '{row[field]}' to float in {self.filename}")
            
            return row
        except StopIteration:
            self.close()
            self.is_finished = True
            return None
        except Exception as e:
            print(f"Error reading from {self.filename}: {str(e)}")
            self.close()
            self.is_finished = True
            return None
    
    def close(self):
        """Close the file if it's open"""
        if self.is_open and self.file:
            self.file.close()
            self.is_open = False
    
    def __str__(self):
        return f"CsvFileStreamer({self.filename}, rows_read={self.rows_read}, is_finished={self.is_finished})"

def process_row(row: Dict[str, Any], time_offset: timedelta) -> Dict[str, Any]:
    """Process a single row by adding metadata and adjusting timestamps"""
    # Create a new dict instead of modifying the original
    processed_row = row.copy()
    
    # Add metadata
    processed_row['processed_at'] = datetime.now().isoformat()
    
    # Rename timestamp to measurement_timestamp and adjust it
    if 'timestamp' in processed_row:
        # Set the base date (now minus the specified offset)
        base_date = datetime.now() - time_offset
        
        # Add the timestamp value (in seconds) to the base date
        seconds_value = processed_row['timestamp']
        measurement_date = base_date + timedelta(seconds=seconds_value)
        
        # Format as ISO 8601 date string
        processed_row['measurement_timestamp'] = measurement_date.isoformat()
        
        # Remove the original timestamp field
        del processed_row['timestamp']
    
    return processed_row

class FileRowProducer:
    """Producer that round-robins between multiple file streamers"""
    def __init__(self, file_paths: List[str], time_offset: timedelta, worker_id: int):
        self.streamers = [CsvFileStreamer(path) for path in file_paths]
        self.time_offset = time_offset
        self.worker_id = worker_id
        self.rows_processed = 0
        self.active_files = len(file_paths)
    
    def process_streams(self):
        """Process rows from all streams in a round-robin fashion"""
        print(f"Worker {self.worker_id}: Starting to process {len(self.streamers)} files")
        
        start_time = time.time()
        last_report_time = start_time
        active_streamers = list(self.streamers)  # Start with all streamers
        
        try:
            # Continue as long as we have active files and shouldn't exit
            while active_streamers and not should_exit.is_set():
                # Round-robin through the active streamers
                for i in range(len(active_streamers) - 1, -1, -1):  # Iterate backwards to safely remove items
                    streamer = active_streamers[i]
                    
                    # Get the next row from this streamer
                    row = streamer.get_next_row()
                    
                    if row is not None:
                        # Process the row
                        processed_row = process_row(row, self.time_offset)
                        key = processed_row.get('device_id', '')
                        
                        # Add to global message queue for rate-limited sending
                        message_queue.put((key, processed_row))
                        self.rows_processed += 1
                    
                    # If this streamer is finished, remove it from the active list
                    if streamer.is_finished:
                        print(f"Worker {self.worker_id}: Completed file {streamer.filename} after {streamer.rows_read} rows")
                        active_streamers.pop(i)
                
                # Progress reporting (every 5 seconds)
                current_time = time.time()
                if current_time - last_report_time >= 5:
                    elapsed = current_time - start_time
                    rate = self.rows_processed / elapsed if elapsed > 0 else 0
                    active_count = len(active_streamers)
                    print(f"Worker {self.worker_id}: Processed {self.rows_processed} rows ({rate:.2f} rows/sec), {active_count} files active")
                    last_report_time = current_time
            
            # Calculate and report final statistics
            total_time = time.time() - start_time
            rate = self.rows_processed / total_time if total_time > 0 else 0
            print(f"Worker {self.worker_id}: Completed all assigned files - {self.rows_processed} rows in {total_time:.2f}s ({rate:.2f} rows/sec)")
        
        except Exception as e:
            print(f"Worker {self.worker_id}: Error processing streams: {str(e)}")
        
        finally:
            # Clean up resources
            for streamer in self.streamers:
                streamer.close()

def kafka_sender_process(rate_limit: float):
    """Process that handles rate-limited sending to Kafka"""
    print(f"Kafka sender: Starting with rate limit of {rate_limit} messages/second")
    
    # Calculate the delay between messages to achieve the desired rate
    delay_per_message = 1.0 / rate_limit if rate_limit > 0 else 0
    
    producer = create_producer()
    messages_sent = 0
    start_time = time.time()
    last_report_time = start_time
    next_send_time = start_time
    
    try:
        while not should_exit.is_set():
            try:
                # Try to get a message from the queue with a timeout
                message = message_queue.get(timeout=0.1)
                
                # Rate limiting: wait until next_send_time if necessary
                current_time = time.time()
                if current_time < next_send_time:
                    time.sleep(next_send_time - current_time)
                
                # Send the message to Kafka
                key, value = message
                producer.send(KAFKA_TOPIC, key=key, value=value)
                
                # Calculate the next send time to maintain the rate
                next_send_time = max(next_send_time + delay_per_message, time.time())
                
                messages_sent += 1
                
                # Periodic progress reporting
                current_time = time.time()
                if current_time - last_report_time >= 5:
                    elapsed = current_time - start_time
                    actual_rate = messages_sent / elapsed if elapsed > 0 else 0
                    queue_size = message_queue.qsize()
                    print(f"Kafka sender: Sent {messages_sent} messages ({actual_rate:.2f} msgs/sec), queue size: {queue_size}")
                    last_report_time = current_time
                    
                    # Adaptive adjustment: periodically flush to prevent too much batching
                    producer.flush()
                
            except queue.Empty:
                # No messages in the queue, sleep briefly
                time.sleep(0.01)
            
            # Check if all workers are done and queue is empty
            if all(not p.is_alive() for p in worker_processes) and message_queue.empty():
                print("All workers finished and queue empty, sender is exiting.")
                break
    
    except Exception as e:
        print(f"Kafka sender: Error: {str(e)}")
    
    finally:
        # Ensure all messages are sent before closing
        producer.flush()
        producer.close()
        
        total_time = time.time() - start_time
        actual_rate = messages_sent / total_time if total_time > 0 else 0
        print(f"Kafka sender: Completed sending {messages_sent} messages in {total_time:.2f}s ({actual_rate:.2f} msgs/sec)")

def worker_process(worker_id: int, file_paths: List[str], time_offset_name: str):
    """Worker process function to handle a subset of files"""
    try:
        # Set up signal handlers in the worker process
        setup_signal_handlers()
        
        # Get the time offset
        time_offset = TIME_OFFSETS.get(time_offset_name, TIME_OFFSETS["1_hour"])
        
        # Create and run the file row producer
        producer = FileRowProducer(file_paths, time_offset, worker_id)
        producer.process_streams()
        
    except Exception as e:
        print(f"Worker {worker_id}: Unexpected error: {str(e)}")

def split_files_among_workers(files: List[str], num_workers: int) -> List[List[str]]:
    """Distribute files evenly among workers in an interleaved fashion"""
    worker_files = [[] for _ in range(num_workers)]
    
    for i, file_path in enumerate(files):
        worker_index = i % num_workers
        worker_files[worker_index].append(file_path)
    
    return worker_files

def main():
    # Load configuration from config file
    time_range, rate_limit = load_config()
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Stream patient data files to Kafka with rate limiting')
    parser.add_argument('--time-offset', choices=TIME_OFFSETS.keys(), default=time_range,
                        help=f'Time offset for measurement timestamps (default: {time_range})')
    parser.add_argument('--workers', type=int, default=multiprocessing.cpu_count(),
                        help='Number of worker processes')
    parser.add_argument('--rate', type=float, default=rate_limit,
                        help=f'Rate limit in messages per second (default: {rate_limit})')
    args = parser.parse_args()
    
    # Set up signal handlers
    setup_signal_handlers()
    
    # Get all CSV files in the patient_data directory
    try:
        csv_files = [
            os.path.join(PATIENT_DATA_DIR, f) 
            for f in os.listdir(PATIENT_DATA_DIR) 
            if f.endswith('.csv')
        ]
    except FileNotFoundError:
        print(f"Directory not found: {PATIENT_DATA_DIR}")
        return
    
    if not csv_files:
        print(f"No CSV files found in {PATIENT_DATA_DIR}")
        return
    
    # Adjust number of workers if we have fewer files than requested workers
    num_workers = min(args.workers, len(csv_files))
    
    print(f"Found {len(csv_files)} CSV files to process with time offset: {args.time_offset}")
    print(f"Using rate limit: {args.rate} msgs/sec, workers: {num_workers}")
    
    # Distribute files among workers in an interleaved fashion
    worker_file_assignments = split_files_among_workers(csv_files, num_workers)
    
    # Start Kafka sender process first
    kafka_sender = multiprocessing.Process(
        target=kafka_sender_process,
        args=(args.rate,)
    )
    kafka_sender.start()
    
    # Start worker processes
    for worker_id, file_paths in enumerate(worker_file_assignments):
        print(f"Worker {worker_id}: Assigned {len(file_paths)} files")
        p = multiprocessing.Process(
            target=worker_process,
            args=(worker_id, file_paths, args.time_offset)
        )
        worker_processes.append(p)
        p.start()
    
    # Wait for all worker processes to complete
    for p in worker_processes:
        p.join()
    
    # Wait for the Kafka sender to complete
    kafka_sender.join()
    
    print("All processing complete")

if __name__ == "__main__":
    # For multiprocessing to work on Windows
    multiprocessing.freeze_support()
    main()