#!/usr/bin/env python3

import os
import csv
import heapq
import argparse
import time
from tqdm import tqdm
from pathlib import Path


def merge_sorted_patient_files(patient_dir, output_file, sort_column, delimiter=',', 
                             quotechar='"', has_header=True, batch_size=1000):
    """
    Merge multiple pre-sorted patient files into a single sorted file.
    
    Args:
        patient_dir (str): Directory containing patient data files
        output_file (str): Path to the output sorted file
        sort_column (str or int): Column name or index to sort by
        delimiter (str): CSV delimiter character
        quotechar (str): CSV quote character
        has_header (bool): Whether the CSV files have a header row
        batch_size (int): Number of rows to write at once for better performance
    """
    # Get all patient files
    patient_files = [os.path.join(patient_dir, f) for f in os.listdir(patient_dir) 
                    if os.path.isfile(os.path.join(patient_dir, f))]
    
    if not patient_files:
        print(f"No files found in {patient_dir}")
        return
    
    print(f"Found {len(patient_files)} patient files to merge")
    
    # Open all files and prepare readers
    file_handles = []
    readers = []
    headers = []
    
    # Set up progress bar for file opening
    open_progress = tqdm(patient_files, desc="Opening files", unit="file")
    
    for patient_file in open_progress:
        try:
            file_handle = open(patient_file, 'r', newline='')
            reader = csv.reader(file_handle, delimiter=delimiter, quotechar=quotechar)
            
            # Process header if it exists
            if has_header:
                header = next(reader)
                headers.append(header)
            
            file_handles.append(file_handle)
            readers.append(reader)
        except Exception as e:
            print(f"Error opening {patient_file}: {e}")
    
    # Verify all headers are the same if they exist
    if has_header and headers:
        if not all(h == headers[0] for h in headers):
            print("Warning: Headers in patient files don't match!")
        header = headers[0]
        
        # If sort_column is a string (column name), convert it to index
        if isinstance(sort_column, str):
            try:
                sort_column_idx = header.index(sort_column)
            except ValueError:
                raise ValueError(f"Column '{sort_column}' not found in the CSV header")
        else:
            sort_column_idx = sort_column
    else:
        header = None
        sort_column_idx = sort_column if isinstance(sort_column, int) else 0
    
    # Open the output file
    with open(output_file, 'w', newline='') as outfile:
        writer = csv.writer(outfile, delimiter=delimiter, quotechar=quotechar)
        
        # Write the header if it exists
        if has_header and header:
            writer.writerow(header)
        
        # Initialize heap and counters for progress tracking
        heap = []
        total_rows_written = 0
        batch_rows = []
        
        # Create a progress bar (we'll update it manually)
        progress = tqdm(desc="Merging files", unit="rows")
        
        # Start time for reporting
        start_time = time.time()
        last_update_time = start_time
        
        # Initialize the heap with the first row from each file
        for i, reader in enumerate(readers):
            try:
                row = next(reader)
                # Store (value to sort by, file index, row)
                heapq.heappush(heap, (row[sort_column_idx], i, row))
            except StopIteration:
                pass  # This file is empty
            except Exception as e:
                print(f"Error reading from file {i}: {e}")
        
        # Process rows
        while heap:
            # Get the smallest row
            val, file_idx, row = heapq.heappop(heap)
            batch_rows.append(row)
            total_rows_written += 1
            
            # Get the next row from the same file
            try:
                next_row = next(readers[file_idx])
                heapq.heappush(heap, (next_row[sort_column_idx], file_idx, next_row))
            except StopIteration:
                pass  # This file is now empty
            except Exception as e:
                print(f"Error reading from file {file_idx}: {e}")
            
            # Write in batches for better performance
            if len(batch_rows) >= batch_size:
                writer.writerows(batch_rows)
                batch_rows = []
            
            # Update progress periodically (not every row to avoid slowdowns)
            if total_rows_written % 10000 == 0:
                progress.update(10000)
                
                # Calculate and display processing speed
                current_time = time.time()
                if current_time - last_update_time >= 5:  # Update every 5 seconds
                    speed = total_rows_written / (current_time - start_time)
                    progress.set_postfix({"rows/sec": f"{speed:.2f}"})
                    last_update_time = current_time
        
        # Write any remaining rows
        if batch_rows:
            writer.writerows(batch_rows)
        
        # Close the progress bar
        progress.update(progress.n % 10000)
        progress.close()
    
    # Close all file handles
    for handle in file_handles:
        handle.close()
    
    elapsed_time = time.time() - start_time
    print(f"\nMerge complete! Wrote {total_rows_written:,} rows in {elapsed_time:.2f} seconds")
    print(f"Average speed: {total_rows_written / elapsed_time:.2f} rows/second")
    print(f"Output file: {output_file}")


def main():
    parser = argparse.ArgumentParser(description='Merge multiple pre-sorted patient files into a single sorted file.')
    parser.add_argument('patient_dir', help='Directory containing patient data files')
    parser.add_argument('output_file', help='Path to the output sorted file')
    parser.add_argument('--sort-column', '-s', required=True, 
                        help='Column name or 0-based index to sort by')
    parser.add_argument('--delimiter', '-d', default=',',
                        help='CSV delimiter character (default: ,)')
    parser.add_argument('--quotechar', '-q', default='"',
                        help='CSV quote character (default: ")')
    parser.add_argument('--no-header', action='store_false', dest='has_header',
                        help='Specify if the CSV files have no header row')
    parser.add_argument('--batch-size', '-b', type=int, default=1000,
                        help='Number of rows to write at once (default: 1000)')

    args = parser.parse_args()
    
    # Check if patient_dir exists
    if not os.path.isdir(args.patient_dir):
        print(f"Error: Directory '{args.patient_dir}' does not exist.")
        return
    
    # Check if sort_column is an integer
    try:
        sort_column = int(args.sort_column)
    except ValueError:
        sort_column = args.sort_column
    
    # Create output directory if it doesn't exist
    output_dir = os.path.dirname(os.path.abspath(args.output_file))
    os.makedirs(output_dir, exist_ok=True)
    
    merge_sorted_patient_files(
        args.patient_dir,
        args.output_file,
        sort_column,
        args.delimiter,
        args.quotechar,
        args.has_header,
        args.batch_size
    )


if __name__ == "__main__":
    main()