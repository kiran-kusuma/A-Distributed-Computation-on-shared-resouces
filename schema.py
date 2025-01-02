import os
import pandas as pd
import json
from pathlib import Path

class DatabaseMetadataGenerator:
    def __init__(self, folder_path: str, output_path: str):
        self.folder_path = folder_path
        self.output_path = output_path
        self.tables_info = {}
        self.relationships = {}
        self.database_metadata = {}

    def identify_relationships(self):
        for filename in os.listdir(self.folder_path):
            full_file_path = os.path.join(self.folder_path, filename)
            
            if os.path.isfile(full_file_path) and filename.endswith('.csv'):
                df = pd.read_csv(full_file_path, encoding='latin-1')  # or 'utf-16'
                columns_and_types = df.dtypes.to_dict()
                self.tables_info[filename] = columns_and_types

        for file1, columns1 in self.tables_info.items():
            for file2, columns2 in self.tables_info.items():
                if file1 != file2:
                    common_columns = set(columns1).intersection(columns2)
                    
                    if common_columns:
                        for common_col in common_columns:
                            df1 = pd.read_csv(os.path.join(self.folder_path, file1),engine='python')
                            df2 = pd.read_csv(os.path.join(self.folder_path, file2),engine='python')
                            
                            matching_values = set(df1[common_col]).intersection(set(df2[common_col]))
                            
                            if matching_values:
                                if file1 not in self.relationships:
                                    self.relationships[file1] = {}
                                if file2 not in self.relationships:
                                    self.relationships[file2] = {}
                                
                                self.relationships[file1][common_col] = (file2, common_col)
                                self.relationships[file2][common_col] = (file1, common_col)

    def build_metadata(self):
        for filename, columns_and_types in self.tables_info.items():
            table_metadata = {
                "columns": []
            }
            
            for column, dtype in columns_and_types.items():
                column_metadata = {
                    "name": column,
                    "type": str(dtype)
                }
                
                if column in self.relationships.get(filename, {}):
                    related_table, related_column = self.relationships[filename][column]
                    column_metadata["references"] = {
                        "table": related_table,
                        "column": related_column
                    }
                
                table_metadata["columns"].append(column_metadata)
            
            self.database_metadata[filename] = table_metadata

    def save_metadata(self):
        with open(self.output_path, 'w') as json_file:
            json.dump(self.database_metadata, json_file, indent=4)
        print(f"Metadata has been saved to {self.output_path}")

    def generate(self):
        self.identify_relationships()
        self.build_metadata()
        self.save_metadata()