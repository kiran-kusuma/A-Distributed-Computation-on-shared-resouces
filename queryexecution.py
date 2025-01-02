import pandas as pd
import json
import os
from pathlib import Path
from typing import List, Dict, Any, Union
import sqlparse
from sqlparse.sql import Where, Comparison, Identifier, Token
import re

class DistributedQueryEngine:
    def __init__(self, database_path: str, metadata_path: str, shard_metadata_path: str):
        """
        Initialize the distributed query engine.
        
        Args:
            database_path: Path to the folder containing sharded CSV files
            metadata_path: Path to the database schema metadata JSON file
            shard_metadata_path: Path to the shard distribution metadata JSON file
        """
        self.database_path = Path(database_path)
        self.schema = self._load_json(metadata_path)
        self.shard_metadata = self._load_json(shard_metadata_path)
        
    def _load_json(self, path: str) -> Dict:
        """Load and parse a JSON file."""
        with open(path, 'r') as f:
            return json.load(f)
    
    def _normalize_column_name(self, column_name: str) -> str:
        """Normalize column names (e.g., replace spaces with underscores)."""
        return column_name.replace("_", " ")
    
    def _parse_where_clause(self, where_clause: Where) -> Dict[str, Any]:
        """Parse WHERE clause to extract conditions."""
        conditions = {}
        if where_clause:
            for token in where_clause.tokens:
                if isinstance(token, Comparison):
                    # Extract column name and value from comparison
                    left = str(token.left).strip()
                    right = str(token.right).strip().strip("'").strip('"')
                    operator = str(token.token_next(0)[1])
                    
                    # Normalize column name
                    left_normalized = self._normalize_column_name(left)
                    
                    conditions[left_normalized] = {'value': right, 'operator': operator}
        return conditions
    
    def _identify_relevant_shards(self, table_name: str, conditions: Dict) -> List[int]:
        """Identify which shards need to be queried based on conditions."""
        if table_name not in self.shard_metadata:
            raise ValueError(f"Table {table_name} not found in shard metadata")
            
        # For now, return all shards - this could be optimized based on conditions
        return [shard['shard_index'] for shard in self.shard_metadata[table_name]['shards']]
    
    def _read_shard(self, shard_index: int) -> pd.DataFrame:
        """Read a specific shard file."""
        shard_path = self.database_path / f'shard_{shard_index}.csv'
        return pd.read_csv(shard_path)
    
    def _apply_conditions(self, df: pd.DataFrame, conditions: Dict) -> pd.DataFrame:
        """Apply WHERE conditions to the DataFrame."""
        for column, condition in conditions.items():
            operator = condition['operator']
            value = condition['value']
            
            # Normalize column name for condition
            column_normalized = self._normalize_column_name(column)
            
            if operator == '=':
                df = df[df[column_normalized] == value]
            elif operator == '>':
                df = df[df[column_normalized] > float(value)]
            elif operator == '<':
                df = df[df[column_normalized] < float(value)]
            elif operator == '>=':
                df = df[df[column_normalized] >= float(value)]
            elif operator == '<=':
                df = df[df[column_normalized] <= float(value)]
                
        return df
    
    def execute_query(self, query: str) -> pd.DataFrame:
        """
        Execute a simplified SQL query across the distributed database.
        
        Currently supports basic SELECT queries with WHERE clauses.
        
        Args:
            query: SQL query string (simplified format)
            
        Returns:
            pandas DataFrame with query results
        """
        # Parse the SQL query
        parsed = sqlparse.parse(query)[0]
        
        # Extract table name
        from_seen = False
        table_name = None
        for token in parsed.tokens:
            if from_seen and isinstance(token, Identifier):
                table_name = str(token).strip()
                break
            if str(token).upper() == 'FROM':
                from_seen = True
                
        if not table_name:
            raise ValueError("Could not identify table name in query")
        
        # Extract WHERE conditions if present
        where_clause = None
        for token in parsed.tokens:
            if isinstance(token, Where):
                where_clause = token
                break
                
        conditions = self._parse_where_clause(where_clause) if where_clause else {}
        
        # Identify relevant shards
        relevant_shards = self._identify_relevant_shards(table_name, conditions)
        
        # Execute query across shards and combine results
        results = []
        for shard_index in relevant_shards:
            shard_df = self._read_shard(shard_index)
            
            # Apply conditions
            if conditions:
                shard_df = self._apply_conditions(shard_df, conditions)
                
            results.append(shard_df)
            
        # Combine results
        if results:
            final_result = pd.concat(results, ignore_index=True)
            
            # Extract selected columns
            select_columns = []
            select_seen = False
            for token in parsed.tokens:
                if select_seen and isinstance(token, Identifier):
                    select_columns.extend([col.strip() for col in str(token).split(',')])
                if str(token).upper() == 'SELECT':
                    select_seen = True
                elif str(token).upper() == 'FROM':
                    break
                    
            if select_columns and select_columns != ['*']:
                final_result = final_result[select_columns]
                
            return final_result
        else:
            return pd.DataFrame()
        
    def get_table_schema(self, table_name: str) -> Dict:
        """Get the schema information for a specific table."""
        if table_name not in self.schema:
            raise ValueError(f"Table {table_name} not found in schema")
        return self.schema[table_name]