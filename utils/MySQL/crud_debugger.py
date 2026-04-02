
import json
import sys
from typing import Dict, List, Tuple, Any, Optional
from datetime import datetime


class MySQLCRUDDebugger:
    def __init__(self, schema_path: str, crud_path: str, output_path: str = None):
        """Initialize debugger with schema and CRUD files."""
        self.schema_path = schema_path
        self.crud_path = crud_path
        self.output_path = output_path or "generated_queries.json"
        
        self.schema = {}
        self.crud_queries = []
        self.generated_queries = []
        self.errors = []
        self.warnings = []
        self.fk_graph = {}  
        
        self._load_files()
        self._build_schema_graph()

    def _load_files(self):
        """Load schema and CRUD JSON files."""
        try:
            with open(self.schema_path, 'r') as f:
                self.schema = json.load(f)
            print(f"Schema loaded: {len(self.schema)} tables")
        except Exception as e:
            self.errors.append(f"Schema load error: {e}")
            
        try:
            with open(self.crud_path, 'r') as f:
                data = json.load(f)
                self.crud_queries = data.get('queries', [])
            print(f"CRUD queries loaded: {len(self.crud_queries)} operations")
        except Exception as e:
            self.errors.append(f"CRUD load error: {e}")

    # ============ SCHEMA GRAPH CONSTRUCTION ============

    def _build_schema_graph(self):
        """
        Build a graph of FK relationships between tables.
        Graph structure: {table: [(target_table, fk_col, ref_col), ...]}
        """
        self.fk_graph = {table: [] for table in self.schema.keys() if not table.startswith('table@')}
        
        for table_name in self.schema.keys():
            if table_name.startswith('table@'):
                continue
            
            fks = self.get_foreign_keys(table_name)
            for fk_col, (ref_table, ref_col) in fks.items():
                if ref_table in self.fk_graph:
                    self.fk_graph[table_name].append((ref_table, fk_col, ref_col))
                    
                    if not any(t == table_name and fc == fk_col for t, fc, _ in self.fk_graph[ref_table]):
                        self.fk_graph[ref_table].append((table_name, ref_col, fk_col))

    def _find_path_bfs(self, start_table: str, end_table: str) -> Optional[List[Tuple[str, str, str, str]]]:
        """
        Find best path between two tables using BFS.
        Prioritizes:
        1. Shortest paths (fewest hops) - ensures minimal JOINs
        2. Among same-length paths: those with junctions > those without
        3. ALL paths must respect actual FK relationships only
        
        Returns: List of (from_table, to_table, from_col, to_col) tuples, or None if no path exists.
        """
        if start_table == end_table:
            return []
        
        if start_table not in self.fk_graph or end_table not in self.fk_graph:
            return None
        
        from collections import deque
        
        def count_junctions(path: List[Tuple[str, str, str, str]]) -> int:
            """Count junction tables (2+ FKs) in path."""
            junctions = 0
            seen = set()
            for _, to_tbl, _, _ in path:
                if to_tbl not in seen:
                    if len(self.get_foreign_keys(to_tbl)) >= 2:
                        junctions += 1
                    seen.add(to_tbl)
            return junctions
        
        queue = deque([(start_table, [])])
        visited = {start_table}
        all_paths = []
        min_length = float('inf')
        
        while queue:
            current, path = queue.popleft()
            
            if len(path) >= min_length:
                continue
            
            for next_table, col1, col2 in self.fk_graph[current]:
                if next_table == end_table:
                    complete_path = path + [(current, next_table, col1, col2)]
                    length = len(complete_path)
                    
                    if length < min_length:
                        min_length = length
                        all_paths = [complete_path]
                    elif length == min_length:
                        all_paths.append(complete_path)
                
                elif next_table not in visited:
                    visited.add(next_table)
                    queue.append((next_table, path + [(current, next_table, col1, col2)]))
        
        if all_paths:
            all_paths.sort(key=lambda p: (-count_junctions(p), len(p)))
            return all_paths[0]
        
        return None


    def get_table_columns(self, table: str) -> Dict[str, List[str]]:
        """Extract all columns from a table schema."""
        if table not in self.schema:
            self.errors.append(f"Table '{table}' not found in schema")
            return {}
        
        schema_entry = self.schema[table]
        columns = {}
        
        for col_name, col_info in schema_entry.items():
            if col_name.startswith('table@'):
                continue
            if isinstance(col_info, list):
                columns[col_name] = col_info
        
        return columns

    def get_primary_keys(self, table: str) -> List[str]:
        """Extract primary key columns from table."""
        columns = self.get_table_columns(table)
        pk_columns = []
        
        for col_name, col_info in columns.items():
            if "PRIMARY KEY" in col_info:
                pk_columns.append(col_name)
        
        return pk_columns

    def get_foreign_keys(self, table: str) -> Dict[str, Tuple[str, str]]:
        """Extract foreign key relationships."""
        columns = self.get_table_columns(table)
        fks = {}
        
        for col_name, col_info in columns.items():
            for info in col_info:
                if "FK ->" in info:
                    # Parse: "FK -> table(column)"
                    parts = info.split("->")[1].strip().split("(")
                    ref_table = parts[0].strip()
                    ref_column = parts[1].rstrip(")").strip()
                    fks[col_name] = (ref_table, ref_column)
        
        return fks

    def is_valid_table(self, table: str) -> bool:
        """Check if table exists in schema."""
        return table in self.schema

    def is_valid_column(self, table: str, column: str) -> bool:
        """Check if column exists in table."""
        columns = self.get_table_columns(table)
        return column in columns

    def get_column_type(self, table: str, column: str) -> Optional[str]:
        """Get column data type."""
        columns = self.get_table_columns(table)
        if column in columns:
            col_info = columns[column]
            return col_info[0] if col_info else None
        return None

    def _find_join_relationship(self, table1: str, table2: str) -> Optional[Tuple[str, str, str]]:
        """
        Find relationship between two tables.
        Returns: (table1_col, table2_col, join_type) or None
        """
        # Check if table2 has a FK to table1
        fks_table2 = self.get_foreign_keys(table2)
        for fk_col, (ref_table, ref_col) in fks_table2.items():
            if ref_table == table1:
                return (ref_col, fk_col, "INNER")
        
        # Check if table1 has a FK to table2
        fks_table1 = self.get_foreign_keys(table1)
        for fk_col, (ref_table, ref_col) in fks_table1.items():
            if ref_table == table2:
                return (fk_col, ref_col, "INNER")
        
        return None

    def _format_value(self, value: Any) -> str:
        """Format value based on type with proper SQL escaping."""
        if value is None:
            return "NULL"
        elif isinstance(value, bool):
            return "1" if value else "0"
        elif isinstance(value, (int, float)):
            return str(value)
        elif isinstance(value, str):
            escaped = value.replace("'", "''")
            return f"'{escaped}'"
        else:
            return f"'{str(value)}'"

    def validate_add_query(self, query: Dict) -> Tuple[bool, List[str]]:
        """Validate INSERT operation."""
        errors = []
        entity = query.get('entity')
        data = query.get('data')
        
        # Check table exists
        if not self.is_valid_table(entity):
            errors.append(f"Table '{entity}' does not exist in schema")
            return False, errors
        
        # Check data exists
        if not data:
            errors.append("No data provided for ADD operation")
            return False, errors
        
        # Handle batch inserts
        data_list = data if isinstance(data, list) else [data]
        
        for idx, record in enumerate(data_list):
            if not isinstance(record, dict):
                errors.append(f"Record {idx} is not a dictionary")
                continue
            
            # Validate each field
            for col_name, col_value in record.items():
                if col_name.startswith('__'):  
                    continue
                
                # Check for nested relationships
                if isinstance(col_value, list) and col_value and isinstance(col_value[0], dict):
                    continue
                
                if not self.is_valid_column(entity, col_name):
                    errors.append(f"Record {idx}: Column '{col_name}' not found in table '{entity}'")
            
            # Check primary key
            pk_columns = self.get_primary_keys(entity)
            for pk_col in pk_columns:
                if pk_col not in record:
                    errors.append(f"Record {idx}: Missing primary key column '{pk_col}'")
        
        return len(errors) == 0, errors

    def validate_get_query(self, query: Dict) -> Tuple[bool, List[str]]:
        """Validate SELECT operation."""
        errors = []
        entity = query.get('entity')
        fields = query.get('fields', ['*'])
        where = query.get('where', {})
        
        # Check table exists
        if not self.is_valid_table(entity):
            errors.append(f"Table '{entity}' does not exist in schema")
            return False, errors
        
        # Validate fields
        for field in fields:
            if field == '*':
                continue
            
            # Handle aggregates: COUNT(ID)
            if '(' in field and ')' in field:
                continue
            
            # Handle nested fields
            if '.' in field:
                parts = field.split('.')
                continue
            
            if not self.is_valid_column(entity, field):
                errors.append(f"Field '{field}' not found in table '{entity}'")
        
        # Validate WHERE conditions
        for col_name, condition in where.items():
            if '.' not in col_name:  
                if not self.is_valid_column(entity, col_name):
                    errors.append(f"WHERE column '{col_name}' not found in table '{entity}'")
        
        return len(errors) == 0, errors

    def validate_change_query(self, query: Dict) -> Tuple[bool, List[str]]:
        """Validate UPDATE operation."""
        errors = []
        entity = query.get('entity')
        where = query.get('where')
        data = query.get('data')
        
        # Check table exists
        if not self.is_valid_table(entity):
            errors.append(f"Table '{entity}' does not exist in schema")
            return False, errors
        
        if not where:
            errors.append("WHERE clause is required for UPDATE")
        
        if not data:
            errors.append("No data provided for CHANGE operation")
            return False, errors
        
        # Validate WHERE columns
        for col_name in where.keys():
            if not self.is_valid_column(entity, col_name):
                errors.append(f"WHERE column '{col_name}' not found in table '{entity}'")
        
        # Validate UPDATE columns
        for col_name in data.keys():
            if not self.is_valid_column(entity, col_name):
                errors.append(f"UPDATE column '{col_name}' not found in table '{entity}'")
        
        return len(errors) == 0, errors

    def validate_remove_query(self, query: Dict) -> Tuple[bool, List[str]]:
        """Validate DELETE operation."""
        errors = []
        entity = query.get('entity')
        where = query.get('where')
        
        # Check table exists
        if not self.is_valid_table(entity):
            errors.append(f"Table '{entity}' does not exist in schema")
            return False, errors
        
        if not where:
            errors.append("WHERE clause is required for DELETE (to prevent deleting all records)")
            return False, errors
        
        # Validate WHERE columns
        for col_name in where.keys():
            if not self.is_valid_column(entity, col_name):
                errors.append(f"WHERE column '{col_name}' not found in table '{entity}'")
        
        return len(errors) == 0, errors

    def generate_insert_sql(self, query: Dict) -> List[Dict]:
        """Generate INSERT SQL statements with proper type handling."""
        queries = []
        entity = query.get('entity')
        data = query.get('data')
        
        data_list = data if isinstance(data, list) else [data]
        
        for record in data_list:
            nested_rels = {}
            flat_data = {}
            
            # Separate nested relationships from regular data
            for col_name, col_value in record.items():
                if isinstance(col_value, list) and col_value and isinstance(col_value[0], dict):
                    nested_rels[col_name] = col_value
                else:
                    flat_data[col_name] = col_value
            
            # Generate main INSERT
            if flat_data:
                columns = ', '.join(flat_data.keys())
                values = ', '.join([self._format_value(v) for v in flat_data.values()])
                sql = f"INSERT INTO {entity} ({columns}) VALUES ({values})"
                
                queries.append({
                    "type": "insert",
                    "entity": entity,
                    "sql": sql,
                    "description": f"Insert {entity} record"
                })
            
            # Generate nested relationship inserts
            for rel_name, rel_records in nested_rels.items():
                if self.is_valid_table(rel_name):
                    for rel_record in rel_records:
                        # Merge primary key from parent
                        pk_columns = self.get_primary_keys(entity)
                        for pk_col in pk_columns:
                            if pk_col in flat_data:
                                rel_record[pk_col] = flat_data[pk_col]
                        
                        # Recursively generate insert for relationship
                        nested_insert = self.generate_insert_sql({
                            'entity': rel_name,
                            'data': rel_record
                        })
                        queries.extend(nested_insert)
        
        return queries

    def _find_join_chain(self, from_table: str, to_table: str) -> Optional[List[Tuple[str, str, str, str]]]:
        """
        Find complete join chain from from_table to to_table using schema graph.
        Uses BFS to find shortest path based on actual FK relationships.
        Returns: List of (from_tbl, to_tbl, from_col, to_col) tuples representing the join chain
        """
        path = self._find_path_bfs(from_table, to_table)
        return path

    def _format_value(self, value: Any) -> str:
        """Format value based on type with proper SQL escaping."""
        if value is None:
            return "NULL"
        elif isinstance(value, bool):
            return "1" if value else "0"
        elif isinstance(value, (int, float)):
            return str(value)
        elif isinstance(value, str):
            escaped = value.replace("'", "''")
            return f"'{escaped}'"
        else:
            return f"'{str(value)}'"

    def generate_select_sql(self, query: Dict) -> List[Dict]:
        """Generate SELECT SQL statements with proper JOINs including transitive relationships."""
        queries = []
        entity = query.get('entity')
        fields = query.get('fields', ['*'])
        where = query.get('where', {})
        options = query.get('options', {})
        
        use_nest = options.get('nest', False)
        
        # Build SELECT clause
        if fields == ['*']:
            select_clause = "*"
        else:
            select_clause = ', '.join(fields)
        
        # Build FROM clause
        from_clause = entity
        
        # Extract all table references from fields and WHERE conditions
        referenced_tables = set()
        
        for field in fields:
            if '.' in field and not field.startswith('('):  
                parts = field.split('.')
                table_name = parts[0]
                if self.is_valid_table(table_name):
                    referenced_tables.add(table_name)
        
        for col_name in where.keys():
            if '.' in col_name:
                parts = col_name.split('.')
                table_name = parts[0]
                if self.is_valid_table(table_name):
                    referenced_tables.add(table_name)
        
        # Build joins for all referenced tables
        joins = []
        joined_tables = {entity}
        
        for ref_table in referenced_tables:
            if ref_table not in joined_tables:
                chain = self._find_join_chain(entity, ref_table)
                
                if chain:
                    for from_tbl, to_tbl, col1, col2 in chain:
                        if to_tbl not in joined_tables:
                            join_sql = f"LEFT JOIN {to_tbl} ON {from_tbl}.{col1} = {to_tbl}.{col2}"
                            if join_sql not in joins:
                                joins.append(join_sql)
                            joined_tables.add(to_tbl)
        
        # Build WHERE clause with proper type handling
        where_conditions = []
        for col_name, condition in where.items():
            if isinstance(condition, dict):
                # Handle operators
                for op, value in condition.items():
                    formatted_value = self._format_value(value)
                    if op == '$gt':
                        where_conditions.append(f"{col_name} > {formatted_value}")
                    elif op == '$lt':
                        where_conditions.append(f"{col_name} < {formatted_value}")
                    elif op == '$gte':
                        where_conditions.append(f"{col_name} >= {formatted_value}")
                    elif op == '$lte':
                        where_conditions.append(f"{col_name} <= {formatted_value}")
                    elif op == '$ne':
                        where_conditions.append(f"{col_name} != {formatted_value}")
                    elif op == '$in':
                        formatted_values = [self._format_value(v) for v in value]
                        where_conditions.append(f"{col_name} IN ({','.join(formatted_values)})")
            else:
                formatted_value = self._format_value(condition)
                where_conditions.append(f"{col_name} = {formatted_value}")
        
        # Build GROUP BY clause
        group_by_clause = ""
        if 'group_by' in options:
            group_by_clause = f"GROUP BY {', '.join(options['group_by'])}"
        
        # Build ORDER BY clause
        order_by_clause = ""
        if 'sort' in options:
            sorts = []
            for sort_field in options['sort']:
                if sort_field.startswith('-'):
                    sorts.append(f"{sort_field[1:]} DESC")
                else:
                    sorts.append(f"{sort_field} ASC")
            order_by_clause = f"ORDER BY {', '.join(sorts)}"
        
        # Build LIMIT clause
        limit_clause = ""
        if 'limit' in options:
            limit_clause = f"LIMIT {options['limit']}"
        
        # Assemble final query
        sql = f"SELECT {select_clause} FROM {from_clause}"
        if joins:
            sql += " " + " ".join(joins)
        if where_conditions:
            sql += " WHERE " + " AND ".join(where_conditions)
        if group_by_clause:
            sql += f" {group_by_clause}"
        if order_by_clause:
            sql += f" {order_by_clause}"
        if limit_clause:
            sql += f" {limit_clause}"
        
        description = f"Select from {entity} with filters and options"
        if use_nest:
            description += " (nested result format)"
        
        queries.append({
            "type": "select",
            "entity": entity,
            "sql": sql,
            "description": description
        })
        
        return queries

    def generate_update_sql(self, query: Dict) -> List[Dict]:
        """Generate UPDATE SQL statements with proper type handling and upsert support."""
        queries = []
        entity = query.get('entity')
        where = query.get('where')
        data = query.get('data')
        options = query.get('options', {})
        
        is_upsert = options.get('upsert', False)
        
        # Build UPDATE clause with proper type formatting
        set_items = []
        for col_name, col_value in data.items():
            formatted_value = self._format_value(col_value)
            set_items.append(f"{col_name} = {formatted_value}")
        
        set_clause = ", ".join(set_items)
        
        # Build WHERE clause with proper type formatting
        where_conditions = []
        for col_name, condition in where.items():
            if isinstance(condition, dict):
                for op, value in condition.items():
                    formatted_value = self._format_value(value)
                    if op == '$lt':
                        where_conditions.append(f"{col_name} < {formatted_value}")
                    elif op == '$gt':
                        where_conditions.append(f"{col_name} > {formatted_value}")
                    elif op == '$lte':
                        where_conditions.append(f"{col_name} <= {formatted_value}")
                    elif op == '$gte':
                        where_conditions.append(f"{col_name} >= {formatted_value}")
                    elif op == '$ne':
                        where_conditions.append(f"{col_name} != {formatted_value}")
            else:
                formatted_value = self._format_value(condition)
                where_conditions.append(f"{col_name} = {formatted_value}")
        
        where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"
        
        # Generate UPDATE query
        sql = f"UPDATE {entity} SET {set_clause} WHERE {where_clause}"
        
        queries.append({
            "type": "update",
            "entity": entity,
            "sql": sql,
            "description": f"Update {entity}" + (" (upsert)" if is_upsert else "")
        })
        
        # If upsert, also generate an INSERT query as fallback
        if is_upsert:
            merged_data = {**where, **data}
            insert_query = self.generate_insert_sql({
                'entity': entity,
                'data': merged_data
            })
            for q in insert_query:
                q['description'] += " (upsert - insert alternative)"
            queries = insert_query + queries
        
        return queries

    def generate_delete_sql(self, query: Dict) -> List[Dict]:
        """Generate DELETE SQL statements with cascade delete support."""
        queries = []
        entity = query.get('entity')
        where = query.get('where')
        options = query.get('options', {})
        
        is_cascade = options.get('cascade', False)
        
        # Build WHERE clause with proper type formatting
        where_conditions = []
        for col_name, condition in where.items():
            if isinstance(condition, dict):
                for op, value in condition.items():
                    formatted_value = self._format_value(value)
                    if op == '$lt':
                        where_conditions.append(f"{col_name} < {formatted_value}")
                    elif op == '$gt':
                        where_conditions.append(f"{col_name} > {formatted_value}")
                    elif op == '$lte':
                        where_conditions.append(f"{col_name} <= {formatted_value}")
                    elif op == '$gte':
                        where_conditions.append(f"{col_name} >= {formatted_value}")
                    elif op == '$ne':
                        where_conditions.append(f"{col_name} != {formatted_value}")
            else:
                formatted_value = self._format_value(condition)
                where_conditions.append(f"{col_name} = {formatted_value}")
        
        where_clause = " AND ".join(where_conditions)
        
        # If cascade, find and delete dependent records first
        if is_cascade:
            # Find all tables that have FK references to this table
            for table_name in self.schema.keys():
                if table_name.startswith('table@'):
                    continue
                
                fks = self.get_foreign_keys(table_name)
                for fk_col, (ref_table, ref_col) in fks.items():
                    if ref_table == entity:
                        # Build a WHERE clause that references the parent delete condition
                        cascade_delete_sql = f"DELETE FROM {table_name} WHERE {fk_col} IN (SELECT {ref_col} FROM {entity} WHERE {where_clause})"
                        
                        queries.append({
                            "type": "delete",
                            "entity": table_name,
                            "sql": cascade_delete_sql,
                            "description": f"Cascade delete from {table_name}"
                        })
        
        # Main delete query
        sql = f"DELETE FROM {entity} WHERE {where_clause}"
        
        queries.append({
            "type": "delete",
            "entity": entity,
            "sql": sql,
            "description": f"Delete from {entity}" + (" (cascade)" if is_cascade else "")
        })
        
        return queries

    def process_all_queries(self) -> bool:
        """Process all CRUD queries: validate and generate SQL."""
        print("\n" + "="*70)
        print("PROCESSING CRUD QUERIES")
        print("="*70 + "\n")
        
        for idx, query_spec in enumerate(self.crud_queries, 1):
            action = query_spec.get('action')
            entity = query_spec.get('entity')
            
            print(f"[{idx}/{len(self.crud_queries)}] {action.upper():<6} | {entity:<12}", end=" | ")
            
            # Validate
            if action == 'add':
                is_valid, errs = self.validate_add_query(query_spec)
            elif action == 'get':
                is_valid, errs = self.validate_get_query(query_spec)
            elif action == 'change':
                is_valid, errs = self.validate_change_query(query_spec)
            elif action == 'remove':
                is_valid, errs = self.validate_remove_query(query_spec)
            else:
                is_valid = False
                errs = [f"Unknown action: {action}"]
            
            if is_valid:
                print("VALID")
                
                # Generate SQL
                if action == 'add':
                    generated = self.generate_insert_sql(query_spec)
                elif action == 'get':
                    generated = self.generate_select_sql(query_spec)
                elif action == 'change':
                    generated = self.generate_update_sql(query_spec)
                elif action == 'remove':
                    generated = self.generate_delete_sql(query_spec)
                
                for gen_query in generated:
                    gen_query['query_index'] = idx
                    self.generated_queries.append(gen_query)
            else:
                print(f"✗ INVALID ({len(errs)} error)")
                for err in errs:
                    self.errors.append(f"Query {idx} ({action} {entity}): {err}")

        return len(self.errors) == 0

    def save_output(self):
        """Save generated queries to JSON file."""
        output_data = {
            "metadata": {
                "generated_at": datetime.now().isoformat(),
                "total_queries": len(self.crud_queries),
                "valid_queries": len([q for q in self.crud_queries]),
                "generated_sql_statements": len(self.generated_queries),
                "errors": len(self.errors),
                "warnings": len(self.warnings)
            },
            "errors": self.errors,
            "warnings": self.warnings,
            "generated_queries": self.generated_queries
        }
        
        try:
            with open(self.output_path, 'w') as f:
                json.dump(output_data, f, indent=2)
            print(f"\nOutput saved to: {self.output_path}")
            return True
        except Exception as e:
            print(f"Failed to save output: {e}")
            return False

    def print_summary(self):
        """Print execution summary."""
        print("\n" + "="*70)
        print("DEBUGGER SUMMARY")
        print("="*70)
        print(f"Total CRUD queries:      {len(self.crud_queries)}")
        print(f"Generated SQL queries:   {len(self.generated_queries)}")
        print(f"Validation errors:       {len(self.errors)}")
        print(f"Warnings:                {len(self.warnings)}")
        
        if self.errors:
            print(f"\n ERRORS ({len(self.errors)}):")
            for i, err in enumerate(self.errors, 1):
                print(f"  {i}. {err}")
        
        if self.warnings:
            print(f"\n WARNINGS ({len(self.warnings)}):")
            for i, warn in enumerate(self.warnings, 1):
                print(f"  {i}. {warn}")
        
        print(f"\n{'='*70}\n")


def main():
    """Main execution."""
    debugger = MySQLCRUDDebugger(
        schema_path='SCHEMA.json', #update the schema path here
        crud_path='sampleCRUD.json', #update the crud path here
        output_path='generated_queries.json'
    )
    
    success = debugger.process_all_queries()
    debugger.print_summary()
    debugger.save_output()
    
    if success:
        print("All queries processed successfully!\n")
    else:
        print("Some queries had validation errors. Check generated_queries.json for details.\n")


if __name__ == "__main__":
    main()
