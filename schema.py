import os
import pandas as pd

# Specify the folder path containing the CSV files
folder_path = '/content/sample_data'

# Create a dictionary to store columns and their types for each CSV
tables_info = {}

# Create a dictionary to store relationships between files and columns
relationships = {}

# Open the schema.txt file for writing
schema_file_path = '/content/schema.txt'
with open(schema_file_path, 'w') as schema_file:
    
    # Step 1: Identify potential relationships first
    for filename in os.listdir(folder_path):
        full_file_path = os.path.join(folder_path, filename)
        
        # Check if it's a CSV file (based on the extension)
        if os.path.isfile(full_file_path) and filename.endswith('.csv'):
            # Load the CSV file using pandas
            df = pd.read_csv(full_file_path)
            
            # Get column names and types
            columns_and_types = df.dtypes.to_dict()
            
            # Store the table name, columns, and their data types
            tables_info[filename] = columns_and_types

    # Compare columns across all files to find common columns
    for file1, columns1 in tables_info.items():
        for file2, columns2 in tables_info.items():
            if file1 != file2:
                common_columns = set(columns1).intersection(columns2)
                
                if common_columns:
                    for common_col in common_columns:
                        # Load the data for the common column
                        df1 = pd.read_csv(os.path.join(folder_path, file1))
                        df2 = pd.read_csv(os.path.join(folder_path, file2))
                        
                        # Check if any values in the common column match between the two files
                        matching_values = set(df1[common_col]).intersection(set(df2[common_col]))
                        
                        if matching_values:
                            # Store the relationship for later use in the schema
                            if file1 not in relationships:
                                relationships[file1] = {}
                            if file2 not in relationships:
                                relationships[file2] = {}
                            
                            relationships[file1][common_col] = (file2, common_col)
                            relationships[file2][common_col] = (file1, common_col)

    # Step 2: Print schema with references based on relationships
    for filename, columns_and_types in tables_info.items():
        schema_str = f"\n{filename} {{\n"
        schema_file.write(schema_str)  # Write to the file
        
        for column, dtype in columns_and_types.items():
            references = ''
            
            # If there is a relationship for this column, add the reference
            if column in relationships.get(filename, {}):
                related_table, related_column = relationships[filename][column]
                references = f' , references {related_table} "{related_column}"'
            
            # Write the column with type and reference if it exists
            column_str = f"    {column}, {dtype}{references} ;\n"
            schema_file.write(column_str)  # Write to the file
        
        schema_file.write("}\n")  # Write closing brace for the schema

print(f"Schema has been saved to {schema_file_path}")