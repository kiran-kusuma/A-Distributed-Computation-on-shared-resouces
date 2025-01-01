import pandas as pd
import os
from pathlib import Path
import numpy as np

def shard_database(input_folder, output_folder, shard_count):
    # Step 1: Create output folder if it doesn't exist
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    # Step 2: Load all tables (CSV files) into a list
    tables = []
    table_names = []
    table_row_counts = {}

    # Load all tables and calculate total rows for each table
    for file_path in Path(input_folder).glob('*.csv'):
        df = pd.read_csv(file_path)
        table_name = file_path.stem
        tables.append(df)
        table_names.append(table_name)
        table_row_counts[table_name] = len(df)

    print(table_row_counts)

    # Step 3: Calculate rows per shard
    k = len(table_names)
    N_values = [1] * k  # Initial distribution of rows
    N = shard_count  # Total number of shards

    # Objective function to compute the weighted cost
    def objective(N_values):
        return sum(table_row_counts[table_names[i]] / N_values[i] for i in range(k))

    # While the sum of N_values is not equal to N, increment the one that maximizes the objective
    current_sum = sum(N_values)
    while current_sum < N:
        # Calculate the current objective values for each table
        obj_values = [table_row_counts[table_names[i]] / N_values[i] for i in range(k)]
    
        # Find the index with the maximum value of n[i] / N_values[i]
        max_index = np.argmax(obj_values)
    
        # Increment the corresponding N_value
        N_values[max_index] += 1
        current_sum += 1  # Update the total sum
    print(N_values)

       

    # Step 4: Initialize variables for row distribution across shards
    shard_data = {i: [] for i in range(shard_count)}  # To store data for each shard
    table_shard_allocation = {}

    # Step 5: Distribute rows into shards (with careful allocation)
    current_shard_index = 0  # Initialize shard index to 0 for the first table
    
    for i, table in enumerate(tables):
        table_name = table_names[i]
        table_row_count = table_row_counts[table_name]

        # Calculate how many rows to allocate to each shard for this table
        rows_for_this_table = table_row_counts[table_name]
        rows_per_shard_for_table = rows_for_this_table / N_values[i]
        remaining_rows_for_table = rows_for_this_table % N_values[i]


        # Store allocation information
        table_shard_allocation[table_name] = {
            'N_value': N_values[i],
            'rows_per_shard': [int(rows_per_shard_for_table)] * N_values[i]
        }

        # Allocate rows to each shard based on N_values for the table
        for j in range(N_values[i]):  # For the N_values of this table
            # Check if we're at the last shard, allocate remaining rows if necessary
            rows_to_allocate = int(rows_per_shard_for_table)  # Integer number of rows to allocate

            # If there are remaining rows, distribute them one by one to the shards
            if j < remaining_rows_for_table:
                rows_to_allocate += 1  # Allocate one more row to this shard
            
            if current_shard_index < shard_count:
                # Add rows to the current shard
                shard_data[current_shard_index].append((table_name, rows_to_allocate))
                print(f"Table: {table_name} | Shard {current_shard_index} | Rows Allocated: {rows_to_allocate}")
                
                # Move to the next shard index
                current_shard_index += 1
            else:
                # If we've reached the maximum number of shards, break
                break


# Example usage
input_folder = '/content/sample_data'  # Folder where original CSV tables are located
output_folder = '/content/sharded_database'  # Folder where sharded tables will be saved
shard_count = 20  # Number of shards to split the tables into

shard_database(input_folder=input_folder, output_folder=output_folder, shard_count=shard_count)