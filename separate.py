import csv
import os
import re
import tempfile
import shutil
import heapq
from collections import defaultdict

# Pre-compile regex for efficiency
PATIENT_REGEX = re.compile(r'(P\d+)')

def extract_patient_id(device_id):
    """Extract patient ID from device_id (format: XXXX_P00YY)"""
    match = PATIENT_REGEX.search(device_id)
    if match:
        return match.group(1)
    return None

def get_sort_key(row, timestamp_idx, delay_idx):
    """Calculate the sort key (timestamp + delay) for a row"""
    try:
        timestamp = float(row[timestamp_idx]) if row[timestamp_idx] else 0.0
        delay = float(row[delay_idx]) if row[delay_idx] else 0.0
        return timestamp + delay
    except (ValueError, IndexError):
        return float('inf')  # If we can't calculate, put it at the end

def sort_csv_by_timestamp_plus_delay(csv_file, timestamp_idx, delay_idx):
    """
    Sort a CSV file by timestamp + delay in ascending order using external merge sort.
    This is memory efficient for large files.
    
    Args:
        csv_file (str): Path to the CSV file
        timestamp_idx (int): Index of the timestamp column
        delay_idx (int): Index of the delay column
    """
    temp_dir = tempfile.mkdtemp()
    try:
        # Read header
        with open(csv_file, 'r', newline='', encoding='utf-8') as f:
            reader = csv.reader(f)
            header = next(reader)
        
        # Step 1: Split file into sorted chunks
        chunk_files = []
        max_chunk_size = 100000  # Adjust based on available memory
        
        with open(csv_file, 'r', newline='', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader)  # skip header
            
            chunk = []
            chunk_count = 0
            for row in reader:
                chunk.append(row)
                if len(chunk) >= max_chunk_size:
                    chunk.sort(key=lambda x: get_sort_key(x, timestamp_idx, delay_idx))
                    chunk_file = os.path.join(temp_dir, f"chunk_{chunk_count}.csv")
                    chunk_files.append(chunk_file)
                    with open(chunk_file, 'w', newline='', encoding='utf-8') as cf:
                        writer = csv.writer(cf)
                        writer.writerows(chunk)
                    chunk = []
                    chunk_count += 1
            
            if chunk:
                chunk.sort(key=lambda x: get_sort_key(x, timestamp_idx, delay_idx))
                chunk_file = os.path.join(temp_dir, f"chunk_{chunk_count}.csv")
                chunk_files.append(chunk_file)
                with open(chunk_file, 'w', newline='', encoding='utf-8') as cf:
                    writer = csv.writer(cf)
                    writer.writerows(chunk)
        
        # Step 2: Merge sorted chunks
        with open(csv_file + '.sorted', 'w', newline='', encoding='utf-8') as out_f:
            writer = csv.writer(out_f)
            writer.writerow(header)
            
            files = [open(f, 'r', newline='', encoding='utf-8') for f in chunk_files]
            readers = [csv.reader(f) for f in files]
            
            heap = []
            for i, reader in enumerate(readers):
                try:
                    row = next(reader)
                    sort_key = get_sort_key(row, timestamp_idx, delay_idx)
                    heapq.heappush(heap, (sort_key, row, i))
                except StopIteration:
                    pass
            
            while heap:
                sort_key, row, idx = heapq.heappop(heap)
                writer.writerow(row)
                try:
                    next_row = next(readers[idx])
                    next_sort_key = get_sort_key(next_row, timestamp_idx, delay_idx)
                    heapq.heappush(heap, (next_sort_key, next_row, idx))
                except StopIteration:
                    pass
            
            for f in files:
                f.close()
        
        shutil.move(csv_file + '.sorted', csv_file)
    finally:
        shutil.rmtree(temp_dir)

def split_csv_by_patient(input_file, output_dir='patient_data'):
    """
    Split a large CSV file into separate files by patient ID.
    
    This improved version writes full rows (including the delay column) to per-patient temporary files.
    It then sorts each file using the external merge sort and finally removes the delay column.
    
    Args:
        input_file (str): Path to the input CSV file
        output_dir (str): Directory to store output files (will be created if it doesn't exist)
    """
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    temp_files = {}  # patient_id -> file handle for full data
    patient_ids = set()
    
    # Process the CSV file in one pass
    with open(input_file, 'r', newline='', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile)
        header = next(reader)
        
        # Determine indices for sorting
        try:
            timestamp_idx = header.index("timestamp")
        except ValueError:
            timestamp_idx = 2  # default to 3rd column if not found
        try:
            delay_idx = header.index("delay")
        except ValueError:
            delay_idx = None
        
        # Create the final header without the delay column (if it exists)
        output_header = [col for i, col in enumerate(header) if delay_idx is not None and i != delay_idx] if delay_idx is not None else header
        
        for row in reader:
            if not row:
                continue
            device_id = row[0]  # Assuming first column
            patient_id = extract_patient_id(device_id)
            if not patient_id:
                print(f"Warning: Could not extract patient ID from device_id: {device_id}")
                continue
            patient_ids.add(patient_id)
            
            # Open a temporary "full" file for this patient if not already open
            if patient_id not in temp_files:
                temp_file_path = os.path.join(output_dir, f"{patient_id}_full.csv")
                temp_files[patient_id] = open(temp_file_path, 'w', newline='', encoding='utf-8')
                writer = csv.writer(temp_files[patient_id])
                writer.writerow(header)  # Write the full header (including delay)
            writer = csv.writer(temp_files[patient_id])
            writer.writerow(row)
    
    # Close all temporary files
    for f in temp_files.values():
        f.close()
    
    # For each patient, sort the full file and then write out the final file without the delay column.
    for patient_id in patient_ids:
        full_file = os.path.join(output_dir, f"{patient_id}_full.csv")
        # Sort the full file in-place (the function moves the sorted file back to full_file)
        if delay_idx is not None:
            sort_csv_by_timestamp_plus_delay(full_file, timestamp_idx, delay_idx)
        else:
            # If there's no delay column, you might want to sort by timestamp only
            sort_csv_by_timestamp_plus_delay(full_file, timestamp_idx, timestamp_idx)
        
        final_file = os.path.join(output_dir, f"{patient_id}.csv")
        with open(full_file, 'r', newline='', encoding='utf-8') as sorted_f, \
             open(final_file, 'w', newline='', encoding='utf-8') as final_f:
            reader = csv.reader(sorted_f)
            writer = csv.writer(final_f)
            
            # Skip the full header and write the modified header without delay
            full_header = next(reader)
            writer.writerow(output_header)
            
            for row in reader:
                if delay_idx is not None:
                    new_row = [val for i, val in enumerate(row) if i != delay_idx]
                else:
                    new_row = row
                writer.writerow(new_row)
        
        os.remove(full_file)
    
    total_rows = sum(1 for _ in open(input_file, encoding='utf-8')) - 1  # minus header
    print(f"Split complete. Processed {total_rows} rows into {len(patient_ids)} patient files.")
    return list(patient_ids)

def get_patient_stats(output_dir='patient_data'):
    """
    Get statistics about the split files.
    
    Args:
        output_dir (str): Directory containing the split files
        
    Returns:
        dict: Mapping of patient_id to row count
    """
    stats = defaultdict(int)
    for filename in os.listdir(output_dir):
        if filename.endswith('.csv') and not filename.endswith('_full.csv'):
            patient_id = filename[:-4]
            with open(os.path.join(output_dir, filename), 'r', encoding='utf-8') as f:
                row_count = sum(1 for _ in f) - 1  # subtract header
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
    patient_ids = split_csv_by_patient(args.input_file, args.output_dir)
    print(f"Successfully split data for {len(patient_ids)} patients.")
    
    stats = get_patient_stats(args.output_dir)
    print("\nPatient statistics:")
    for patient_id, count in sorted(stats.items(), key=lambda x: x[1], reverse=True):
        print(f"{patient_id}: {count} rows")
