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

def sort_csv_by_timestamp_plus_delay(csv_file, timestamp_idx, delay_idx):
    """
    Sort a CSV file by timestamp + delay in ascending order using external merge sort.
    This is memory efficient for large files.
    
    Args:
        csv_file (str): Path to the CSV file
        timestamp_idx (int): Index of the timestamp column
        delay_idx (int): Index of the delay column
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
                    # Sort chunk by timestamp + delay and write to temp file
                    chunk.sort(key=lambda x: get_sort_key(x, timestamp_idx, delay_idx))
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
                chunk.sort(key=lambda x: get_sort_key(x, timestamp_idx, delay_idx))
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
                    # Push (sort_key, row_data, chunk_index) to heap
                    sort_key = get_sort_key(row, timestamp_idx, delay_idx)
                    heapq.heappush(heap, (sort_key, row, i))
                except StopIteration:
                    pass
            
            # Merge chunks
            while heap:
                sort_key, row, chunk_idx = heapq.heappop(heap)
                writer.writerow(row)
                
                try:
                    next_row = next(readers[chunk_idx])
                    next_sort_key = get_sort_key(next_row, timestamp_idx, delay_idx)
                    heapq.heappush(heap, (next_sort_key, next_row, chunk_idx))
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

def get_sort_key(row, timestamp_idx, delay_idx):
    """Calculate the sort key (timestamp + delay) for a row"""
    try:
        timestamp = float(row[timestamp_idx]) if row[timestamp_idx] else 0.0
        delay = float(row[delay_idx]) if row[delay_idx] else 0.0
        return timestamp + delay
    except (ValueError, IndexError):
        return float('inf')  # If we can't calculate, put it at the end

def split_csv_by_patient(input_file, output_dir='patient_data'):
    """
    Split a large CSV file into separate files by patient ID.
    
    Args:
        input_file (str): Path to the input CSV file
        output_dir (str): Directory to store output files (will be created if doesn't exist)
        sort_by_timestamp_plus_delay (bool): Whether to sort each patient's data by timestamp + delay
        exclude_delay (bool): Whether to exclude the delay column from output files
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
        
        # Find indices for timestamp and delay columns
        timestamp_idx = header.index("timestamp") if "timestamp" in header else 2  # Default to 3rd column
        delay_idx = header.index("delay") if "delay" in header else None
        
        if delay_idx is None:
            print("Warning: No 'delay' column found in the header. Defaulting to standard timestamp sorting.")
        
        # Create a new header without the delay column
        if delay_idx is not None:
            output_header = [col for i, col in enumerate(header) if i != delay_idx]
        else:
            output_header = header
        
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
                    
                    # Write header without delay column if requested
                    writers[patient_id].writerow(output_header)
                
                # Write data row without delay column
                if delay_idx is not None:
                    output_row = [val for i, val in enumerate(row) if i != delay_idx]
                    writers[patient_id].writerow(output_row)
                else:
                    writers[patient_id].writerow(row)
                
            except Exception as e:
                print(f"Error processing row: {row}. Error: {e}")
    
    # Close all output files
    for file in output_files.values():
        file.close()
    
    # Sort each patient's file by timestamp + delay
    if delay_idx is not None:
        print("Sorting patient files by timestamp + delay...")
        
        for patient_id in output_files.keys():
            patient_file = os.path.join(output_dir, f"{patient_id}.csv")
            
            # For the sorting, we have already excluded the delay column, 
            # so we need to adjust indices if they've been shifted
            # If the delay column was removed, timestamp_idx needs to be adjusted if it came after delay_idx
            sort_timestamp_idx = timestamp_idx if timestamp_idx < delay_idx else timestamp_idx - 1
                
            # For sorting, we need to temporarily re-add the delay column
            # This is a complex operation that would require modifying the files again
            # Instead, we'll sort the original data before excluding columns
            
            # We'll use temporary files to restore the full data, sort, then remove delay again
            temp_file = os.path.join(output_dir, f"{patient_id}_temp.csv")
            
            # Create a temporary CSV with the full data
            with open(input_file, 'r', newline='', encoding='utf-8') as orig_f, \
                    open(temp_file, 'w', newline='', encoding='utf-8') as temp_f:
                orig_reader = csv.reader(orig_f)
                temp_writer = csv.writer(temp_f)
                
                # Write header with delay to temp file
                temp_writer.writerow(header)
                
                # Filter rows for this patient and write to temp file
                for row in orig_reader:
                    if not row:
                        continue
                    row_device_id = row[0]
                    row_patient_id = extract_patient_id(row_device_id)
                    if row_patient_id == patient_id:
                        temp_writer.writerow(row)
            
            # Sort the temporary file by timestamp + delay
            sort_csv_by_timestamp_plus_delay(temp_file, timestamp_idx, delay_idx)
            
            # Now write back to the patient file without the delay column
            with open(temp_file, 'r', newline='', encoding='utf-8') as temp_f, \
                    open(patient_file, 'w', newline='', encoding='utf-8') as patient_f:
                temp_reader = csv.reader(temp_f)
                patient_writer = csv.writer(patient_f)
                
                # Skip header in temp file
                next(temp_reader)
                
                # Write header without delay to patient file
                patient_writer.writerow(output_header)
                
                # Write rows without delay column
                for row in temp_reader:
                    output_row = [val for i, val in enumerate(row) if i != delay_idx]
                    patient_writer.writerow(output_row)
            
            # Remove the temporary file
            os.remove(temp_file)

    
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
    
    args = parser.parse_args()
    
    print(f"Splitting {args.input_file} by patient ID...")
    patient_ids = split_csv_by_patient(
        args.input_file, 
        args.output_dir
    )
    
    print(f"Successfully split data for {len(patient_ids)} patients.")
    
    # Print stats
    stats = get_patient_stats(args.output_dir)
    print("\nPatient statistics:")
    for patient_id, count in sorted(stats.items(), key=lambda x: x[1], reverse=True):
        print(f"{patient_id}: {count} rows")