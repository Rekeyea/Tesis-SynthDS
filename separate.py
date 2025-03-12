import csv
import os
import re
import tempfile
import shutil
import heapq
from collections import defaultdict

def extract_patient_id(device_id):
    """Extract patient ID from device_id (format: XXXX_P00YY)"""
    match = re.search(r'(P\d+)', device_id)
    if match:
        return match.group(1)
    return None

def sort_csv_by_timestamp(csv_file, timestamp_idx):
    """
    Sort a CSV file by timestamp in ascending order using external merge sort.
    This is memory efficient for large files.
    
    Args:
        csv_file (str): Path to the CSV file
        timestamp_idx (int): Index of the timestamp column
    """
    # Create a temporary directory for chunks
    temp_dir = tempfile.mkdtemp()
    try:
        # Read header
        with open(csv_file, 'r', newline='', encoding='utf-8') as f:
            reader = csv.reader(f)
            header = next(reader)
        
        # Step 1: Split the file into sorted chunks
        chunk_files = []
        max_chunk_size = 100000  # Adjust based on available memory
        
        with open(csv_file, 'r', newline='', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader)  # Skip header
            
            chunk = []
            chunk_count = 0
            
            for row in reader:
                if len(chunk) >= max_chunk_size:
                    # Sort chunk and write to temp file
                    chunk.sort(key=lambda x: float(x[timestamp_idx]) if x[timestamp_idx] else float('inf'))
                    chunk_file = os.path.join(temp_dir, f"chunk_{chunk_count}.csv")
                    chunk_files.append(chunk_file)
                    
                    with open(chunk_file, 'w', newline='', encoding='utf-8') as chunk_f:
                        writer = csv.writer(chunk_f)
                        writer.writerows(chunk)
                    
                    chunk = []
                    chunk_count += 1
                
                chunk.append(row)
            
            # Write the last chunk if not empty
            if chunk:
                chunk.sort(key=lambda x: float(x[timestamp_idx]) if x[timestamp_idx] else float('inf'))
                chunk_file = os.path.join(temp_dir, f"chunk_{chunk_count}.csv")
                chunk_files.append(chunk_file)
                
                with open(chunk_file, 'w', newline='', encoding='utf-8') as chunk_f:
                    writer = csv.writer(chunk_f)
                    writer.writerows(chunk)
        
        # Step 2: Merge sorted chunks
        with open(csv_file + '.sorted', 'w', newline='', encoding='utf-8') as out_f:
            writer = csv.writer(out_f)
            writer.writerow(header)
            
            # Create file objects and readers
            files = [open(f, 'r', newline='', encoding='utf-8') for f in chunk_files]
            readers = [csv.reader(f) for f in files]
            
            # Initialize heap with first row from each chunk
            heap = []
            for i, reader in enumerate(readers):
                try:
                    row = next(reader)
                    # Push (timestamp, row_data, chunk_index) to heap
                    timestamp = float(row[timestamp_idx]) if row[timestamp_idx] else float('inf')
                    heapq.heappush(heap, (timestamp, row, i))
                except StopIteration:
                    pass
            
            # Merge chunks
            while heap:
                timestamp, row, chunk_idx = heapq.heappop(heap)
                writer.writerow(row)
                
                try:
                    next_row = next(readers[chunk_idx])
                    next_timestamp = float(next_row[timestamp_idx]) if next_row[timestamp_idx] else float('inf')
                    heapq.heappush(heap, (next_timestamp, next_row, chunk_idx))
                except StopIteration:
                    pass
            
            # Close file objects
            for f in files:
                f.close()
        
        # Replace original file with sorted file
        shutil.move(csv_file + '.sorted', csv_file)
        
    finally:
        # Clean up temp directory
        shutil.rmtree(temp_dir)

def split_csv_by_patient(input_file, output_dir='patient_data', sort_by_timestamp=True):
    """
    Split a large CSV file into separate files by patient ID.
    
    Args:
        input_file (str): Path to the input CSV file
        output_dir (str): Directory to store output files (will be created if doesn't exist)
        sort_by_timestamp (bool): Whether to sort each patient's data by timestamp
    """
    # Create output directory if it doesn't exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # Keep track of output files
    output_files = {}
    writers = {}
    
    # Process the CSV file line by line
    with open(input_file, 'r', newline='', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile)
        
        # Read header
        header = next(reader)
        
        # Process each row
        row_count = 0
        for row in reader:
            row_count += 1
            if row_count % 100000 == 0:
                print(f"Processed {row_count} rows...")
            
            if not row:  # Skip empty rows
                continue
                
            try:
                device_id = row[0]  # Assuming device_id is the first column
                patient_id = extract_patient_id(device_id)
                
                if not patient_id:
                    print(f"Warning: Could not extract patient ID from device_id: {device_id}")
                    continue
                
                # Create a new writer if we haven't seen this patient before
                if patient_id not in output_files:
                    output_file = os.path.join(output_dir, f"{patient_id}.csv")
                    output_files[patient_id] = open(output_file, 'w', newline='', encoding='utf-8')
                    writers[patient_id] = csv.writer(output_files[patient_id])
                    
                    # Write header
                    writers[patient_id].writerow(header)
                
                # Write data row
                writers[patient_id].writerow(row)
                
            except Exception as e:
                print(f"Error processing row: {row}. Error: {e}")
    
    # Close all output files
    for file in output_files.values():
        file.close()
    
    # Sort each patient's file by timestamp if requested
    if sort_by_timestamp:
        print("Sorting patient files by timestamp...")
        timestamp_idx = header.index("timestamp") if "timestamp" in header else 2  # Default to 3rd column if not found
        
        for patient_id in output_files.keys():
            patient_file = os.path.join(output_dir, f"{patient_id}.csv")
            sort_csv_by_timestamp(patient_file, timestamp_idx)
    
    print(f"Split complete. {row_count} rows processed into {len(output_files)} patient files.")
    patient_ids = list(output_files.keys())
    return patient_ids

def get_patient_stats(output_dir='patient_data'):
    """
    Get statistics about the split files.
    
    Args:
        output_dir (str): Directory containing the split files
        
    Returns:
        dict: Statistics about the patients
    """
    stats = defaultdict(int)
    
    for filename in os.listdir(output_dir):
        if filename.endswith('.csv'):
            patient_id = filename[:-4]  # Remove .csv extension
            
            with open(os.path.join(output_dir, filename), 'r', encoding='utf-8') as f:
                # Subtract 1 for the header
                row_count = sum(1 for _ in f) - 1
                
            stats[patient_id] = row_count
    
    return stats

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Split a large CSV file by patient ID')
    parser.add_argument('input_file', help='Path to the input CSV file')
    parser.add_argument('--output-dir', default='patient_data', 
                        help='Directory to store output files (default: patient_data)')
    parser.add_argument('--no-sort', action='store_true',
                        help='Skip sorting the data by timestamp')
    
    args = parser.parse_args()
    
    print(f"Splitting {args.input_file} by patient ID...")
    patient_ids = split_csv_by_patient(args.input_file, args.output_dir, sort_by_timestamp=not args.no_sort)
    
    print(f"Successfully split data for {len(patient_ids)} patients.")
    
    # Print stats
    stats = get_patient_stats(args.output_dir)
    print("\nPatient statistics:")
    for patient_id, count in sorted(stats.items(), key=lambda x: x[1], reverse=True):
        print(f"{patient_id}: {count} rows")