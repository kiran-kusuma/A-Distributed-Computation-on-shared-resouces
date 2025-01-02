import pandas as pd
import os
from pathlib import Path
import numpy as np
import json

class DatabaseShardGenerator:
    def __init__(self, input_folder: str, output_folder: str, output_folder2: str, shard_count: int):
        self.input_folder = input_folder
        self.output_folder = output_folder
        self.output_folder2 = output_folder2
        self.shard_count = shard_count
        self.tables = []
        self.table_names = []
        self.table_row_counts = {}
        self.N_values = []
        self.shard_data = {}
        self.table_shard_allocation = {}
        self.table_metadata = {}

    def load_tables(self):
        for file_path in Path(self.input_folder).glob('*.csv'):
            df = pd.read_csv(file_path)
            table_name = file_path.stem
            self.tables.append(df)
            self.table_names.append(table_name)
            self.table_row_counts[table_name] = len(df)

    def calculate_shard_distribution(self):
        k = len(self.table_names)
        self.N_values = [1] * k
        current_sum = sum(self.N_values)
        while current_sum < self.shard_count:
            obj_values = [self.table_row_counts[self.table_names[i]] / self.N_values[i] for i in range(k)]
            max_index = np.argmax(obj_values)
            self.N_values[max_index] += 1
            current_sum += 1

    def distribute_rows(self):
        self.shard_data = {i: [] for i in range(self.shard_count)}
        current_shard_index = 0
        for i, table in enumerate(self.tables):
            table_name = self.table_names[i]
            table_row_count = self.table_row_counts[table_name]

            rows_for_this_table = self.table_row_counts[table_name]
            rows_per_shard_for_table = rows_for_this_table / self.N_values[i]
            remaining_rows_for_table = rows_for_this_table % self.N_values[i]

            self.table_shard_allocation[table_name] = {
                'N_value': self.N_values[i],
                'rows_per_shard': [int(rows_per_shard_for_table)] * self.N_values[i]
            }

            self.table_metadata[table_name] = {
                'total_rows': table_row_count,
                'total_shards': self.N_values[i],
                'shards': []
            }

            current_row_start = 0
            for j in range(self.N_values[i]):
                rows_to_allocate = int(rows_per_shard_for_table)
                if j < remaining_rows_for_table:
                    rows_to_allocate += 1

                current_row_end = current_row_start + rows_to_allocate - 1
                self.table_metadata[table_name]['shards'].append({
                    'shard_index': current_shard_index,
                    'row_range': f"{current_row_start}-{current_row_end}",
                    'total_rows': rows_to_allocate
                })

                current_row_start = current_row_end + 1
                self.shard_data[current_shard_index].append((table_name, rows_to_allocate))
                
                current_shard_index += 1
                if current_shard_index == self.shard_count:
                    break

    def save_shards(self):
        for shard_index, shard_rows in self.shard_data.items():
            shard_rows_data = []
            
            for table_name, rows_to_allocate in shard_rows:
                table_df = self.tables[self.table_names.index(table_name)]
                rows = table_df.head(rows_to_allocate)
                shard_rows_data.append(rows)

            shard_df = pd.concat(shard_rows_data, ignore_index=True)
            
            shard_file_path = os.path.join(self.output_folder, f'shard_{shard_index}.csv')
            shard_df.to_csv(shard_file_path, index=False)

    def save_metadata(self):
        metadata_file_path = os.path.join(self.output_folder2, 'shard_metadata.json')
        with open(metadata_file_path, 'w') as json_file:
            json.dump(self.table_metadata, json_file, indent=4)

    def generate(self):
        if not os.path.exists(self.output_folder):
            os.makedirs(self.output_folder)

        self.load_tables()
        self.calculate_shard_distribution()
        self.distribute_rows()
        self.save_shards()
        self.save_metadata()

# Example usage
# input_folder = '/content/sample_data'
# output_folder = '/content/sharded_database'
# shard_count = 20
# shard_generator = DatabaseShardGenerator(input_folder, output_folder, shard_count)
# shard_generator.generate()