"""
SchemaManager: Unified schema inference, PK tracking, and operation generation wrapper.
Integrates SchemaInfere logic into a format consumable by executors and schedulers.
"""
from collections import defaultdict, deque
from typing import Dict, Any, List, Set, Tuple, Optional
from utils.log import schema_manager_logger as logger
from utils.schema_maker import SchemaInfere


class SchemaManager:
    """
    Wrapper around SchemaInfere providing schema awareness, PK tracking, and operation generation.
    Tracks seen primary keys per table and discriminates between INSERT and UPDATE operations.
    """

    def __init__(self, unique_fields: List[str], global_key: str = "record_id", output_dir: str = "."):
        """
        Initialize schema manager.
        
        Args:
            unique_fields: List of field names that serve as unique identifiers/primary keys
            global_key: Field that globally identifies records across tables
            output_dir: Directory for schema and operations logging
        """
        self.unique_fields = set(unique_fields)
        self.global_key = global_key
        self.output_dir = output_dir
        
        # Initialize SchemaInfere for schema detection
        self.schema_inferer = SchemaInfere(
            unique_fields=unique_fields,
            global_key=global_key,
            output_dir=output_dir
        )
        
        # Primary key tracking: table_name -> set of seen PK values
        self.seen_pks: Dict[str, Set[Any]] = defaultdict(set)
        
        # Current schema cache
        self._schema: Optional[Dict[str, Any]] = None
        
        # Track field -> table ownership for conflict resolution
        self.field_to_table: Dict[str, str] = {}

    def ingest_record(self, record: Dict[str, Any]) -> None:
        """
        Ingest a record for schema inference.
        
        Args:
            record: Data record to analyze
        """
        self.schema_inferer.add_record(record)

    def ingest_records_batch(self, records: List[Dict[str, Any]]) -> None:
        """
        Ingest multiple records.
        
        Args:
            records: List of records to analyze
        """
        for record in records:
            self.ingest_record(record)

    def build_schema(self) -> Dict[str, Any]:
        """
        Build and cache the schema based on ingested records.
        
        Returns:
            Schema dict with tables, relationships, and constraints
        """
        self.schema_inferer.flush()
        self._schema = self.schema_inferer.build_schema()
        self._build_field_to_table_mapping()
        return self._schema

    def _build_field_to_table_mapping(self) -> None:
        """
        Build a mapping of fields to their owner tables for conflict resolution.
        """
        if not self._schema:
            return
        
        self.field_to_table = {}
        for table_name, table_def in self._schema.get("tables", {}).items():
            for col in table_def.get("columns", []):
                if col not in self.field_to_table:
                    self.field_to_table[col] = table_name

    def get_table_for_field(self, field: str) -> Optional[str]:
        """
        Resolve which table a field belongs to.
        
        Args:
            field: Field name
            
        Returns:
            Table name or None if unresolved
        """
        return self.field_to_table.get(field)

    def get_primary_key_for_table(self, table_name: str) -> Optional[str]:
        """
        Get the primary key field for a table.
        
        Args:
            table_name: Table name
            
        Returns:
            Primary key field name or None
        """
        if not self._schema:
            return None
        
        table_def = self._schema.get("tables", {}).get(table_name, {})
        return table_def.get("primary_key")

    def is_first_insert_for_pk(self, table_name: str, pk_value: Any) -> bool:
        """
        Check if this is the first time we're seeing this primary key value.
        Used to discriminate between INSERT and UPDATE operations.
        
        Args:
            table_name: Table name
            pk_value: Primary key value
            
        Returns:
            True if this is the first occurrence, False if we've seen it before
        """
        if pk_value not in self.seen_pks[table_name]:
            self.seen_pks[table_name].add(pk_value)
            return True
        return False

    def mark_pk_inserted(self, table_name: str, pk_value: Any) -> None:
        """
        Explicitly mark a primary key as inserted.
        
        Args:
            table_name: Table name
            pk_value: Primary key value
        """
        self.seen_pks[table_name].add(pk_value)

    def unmark_pk(self, table_name: str, pk_value: Any) -> None:
        """
        Remove a primary key value from tracking (e.g., after DELETE).
        
        Args:
            table_name: Table name
            pk_value: Primary key value
        """
        self.seen_pks[table_name].discard(pk_value)

    def get_operation_type_for_record(
        self, 
        record: Dict[str, Any], 
        table_name: str
    ) -> str:
        """
        Determine if a record should be INSERT or UPDATE based on PK tracking.
        
        Args:
            record: Record data
            table_name: Target table name
            
        Returns:
            "INSERT" or "UPDATE"
        """
        pk_field = self.get_primary_key_for_table(table_name)
        if not pk_field or pk_field not in record:
            return "INSERT"  # Default to INSERT if we can't determine
        
        pk_value = record[pk_field]
        return "INSERT" if self.is_first_insert_for_pk(table_name, pk_value) else "UPDATE"

    def generate_sql_insert(
        self, 
        record: Dict[str, Any], 
        table_name: str, 
        is_upsert: bool = True
    ) -> str:
        """
        Generate SQL INSERT statement (with optional ON DUPLICATE KEY UPDATE for upsert).
        
        Args:
            record: Record data
            table_name: Target table name
            is_upsert: If True, adds ON DUPLICATE KEY UPDATE clause
            
        Returns:
            SQL INSERT statement
        """
        if not self._schema:
            logger.warning("Schema not built; cannot generate SQL")
            return ""
        
        table_def = self._schema.get("tables", {}).get(table_name, {})
        columns = table_def.get("columns", [])
        
        # Filter record to include only columns in schema
        values = []
        col_names = []
        for col in columns:
            if col in record:
                col_names.append(col)
                val = record[col]
                if isinstance(val, (dict, list)):
                    import json
                    values.append(f"'{json.dumps(val)}'")
                else:
                    values.append(f"'{val}'")
        
        if not col_names:
            return ""
        
        cols_str = ", ".join([f"`{c}`" for c in col_names])
        vals_str = ", ".join(values)
        sql = f"INSERT INTO `{table_name}` ({cols_str}) VALUES ({vals_str})"
        
        if is_upsert:
            updates = []
            pk = table_def.get("primary_key")
            for col in col_names:
                if col != pk:
                    updates.append(f"`{col}`=VALUES(`{col}`)")
            
            if updates:
                sql += " ON DUPLICATE KEY UPDATE " + ", ".join(updates)
        
        return sql + ";"

    def generate_sql_update(
        self, 
        record: Dict[str, Any], 
        table_name: str,
        criteria: Dict[str, Any]
    ) -> str:
        """
        Generate SQL UPDATE statement.
        
        Args:
            record: Record with updated values
            table_name: Target table name
            criteria: WHERE criteria (field -> value mapping)
            
        Returns:
            SQL UPDATE statement
        """
        if not self._schema:
            logger.warning("Schema not built; cannot generate SQL")
            return ""
        
        table_def = self._schema.get("tables", {}).get(table_name, {})
        columns = table_def.get("columns", [])
        
        # Build SET clause
        set_parts = []
        for col in columns:
            if col in record and col != table_def.get("primary_key"):
                val = record[col]
                if isinstance(val, (dict, list)):
                    import json
                    val = json.dumps(val)
                set_parts.append(f"`{col}`='{val}'")
        
        if not set_parts:
            return ""
        
        # Build WHERE clause
        where_parts = []
        for field, value in criteria.items():
            if isinstance(value, (dict, list)):
                import json
                value = json.dumps(value)
            where_parts.append(f"`{field}`='{value}'")
        
        sql = f"UPDATE `{table_name}` SET " + ", ".join(set_parts)
        if where_parts:
            sql += " WHERE " + " AND ".join(where_parts)
        
        return sql + ";"

    def generate_sql_delete(
        self, 
        table_name: str, 
        criteria: Dict[str, Any]
    ) -> str:
        """
        Generate SQL DELETE statement.
        
        Args:
            table_name: Target table name
            criteria: WHERE criteria
            
        Returns:
            SQL DELETE statement
        """
        where_parts = []
        for field, value in criteria.items():
            if isinstance(value, (dict, list)):
                import json
                value = json.dumps(value)
            where_parts.append(f"`{field}`='{value}'")
        
        if not where_parts:
            logger.warning("DELETE without criteria; skipping")
            return ""
        
        sql = f"DELETE FROM `{table_name}` WHERE " + " AND ".join(where_parts)
        return sql + ";"

    def get_schema(self) -> Optional[Dict[str, Any]]:
        """
        Get the current cached schema.
        
        Returns:
            Schema dict or None if not built
        """
        return self._schema

    def get_foreign_keys(self) -> List[Tuple[str, str]]:
        """
        Get all foreign key relationships.
        
        Returns:
            List of (source_field, referenced_field) tuples
        """
        if not self._schema:
            return []
        return self._schema.get("foreign_keys", [])

    def get_many_to_many_fields(self) -> List[str]:
        """
        Get all fields that represent many-to-many relationships (list columns).
        
        Returns:
            List of field names
        """
        if not self._schema:
            return []
        return self._schema.get("many_to_many", [])

    def get_functional_dependencies(self) -> Dict[str, List[str]]:
        """
        Get functional dependencies discovered by schema inference.
        
        Returns:
            Dict mapping unique_fields to dependent fields
        """
        if not self._schema:
            return {}
        return self._schema.get("functional_dependencies", {})
