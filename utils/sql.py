import json
import os
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set
from collections import defaultdict

import mysql.connector
from mysql.connector import Error
from utils.log import sql_logger as logger


def _quote_identifier(name: str) -> str:
	return f"`{name.replace('`', '``')}`"


def _sql_type(type_name: Optional[str]) -> str:
	mapping = {
		"int": "BIGINT",
		"float": "DOUBLE",
		"bool": "BOOLEAN",
		"str": "TEXT",
		"UNK": "TEXT",
		None: "TEXT",
	}
	return mapping.get(type_name, "TEXT")


class SQLUpdateOrderExecutor:
	def __init__(
		self,
		host: str,
		port: int,
		user: str,
		password: str,
		database: str,
		schema_manager=None,
	):
		logger.info(f"Initializing SQL executor: {user}@{host}:{port}/{database}")
		self._connection = mysql.connector.connect(
			host=host,
			port=port,
			user=user,
			password=password,
			database=database,
			autocommit=False,
		)
		logger.info("Connected to MySQL successfully")
		# Optional schema manager for schema-aware operations and PK tracking
		self.schema_manager = schema_manager
		# Track seen PKs per table: table_name -> set of PK values
		self._seen_pks: Dict[str, Set[Any]] = defaultdict(set)

	def mark_pk_inserted(self, table_name: str, pk_value: Any) -> None:
		"""Track that a primary key has been inserted."""
		self._seen_pks[table_name].add(pk_value)

	def is_first_insert(self, table_name: str, pk_value: Any) -> bool:
		"""Check if this PK value has been seen before."""
		return pk_value not in self._seen_pks[table_name]

	def unmark_pk(self, table_name: str, pk_value: Any) -> None:
		"""Remove a PK from tracking (e.g., after DELETE)."""
		self._seen_pks[table_name].discard(pk_value)

	def get_schema(self) -> Optional[Dict]:
		"""Get the current schema from schema manager."""
		if self.schema_manager:
			return self.schema_manager.get_schema()
		return None

	def close(self) -> None:
		if self._connection.is_connected():
			self._connection.close()

	def execute_update_order(self, update_order: Iterable[Dict[str, Any]]) -> None:
		cursor = self._connection.cursor()
		command_count = 0
		try:
			logger.info("Starting SQL update order processing...")
			for command in update_order:
				if command.get("Executer") != "SQL":
					continue

				command_type = command.get("type")
				table_name = command.get("table_name", "unknown")
				command_count += 1
				logger.debug(f"Executing SQL {command_type} on {table_name} (cmd #{command_count})")
				
				if command_type == "CREATE":
					self._execute_create(cursor, command)
				elif command_type == "ALTER":
					self._execute_alter(cursor, command)
				elif command_type == "INSERT":
					self._execute_insert(cursor, command)
				elif command_type == "UPDATE":
					self._execute_update(cursor, command)
				elif command_type in {"DELETE", "REMOVE"}:
					self._execute_delete(cursor, command)

			self._connection.commit()
			logger.info(f"SQL update order complete. Executed {command_count} commands")
		except Exception as e:
			self._connection.rollback()
			logger.error("Error executing update order, rolled back transaction", exc_info=True)  
			logger.error(f"Failed command: {command}")
			logger.error(f"Error details: {e}")  
			raise
		finally:
			cursor.close()

	def _execute_create(self, cursor, command: Dict[str, Any]) -> None:
		table_name = _quote_identifier(command["table_name"])
		columns = command.get("columns", ["table_autogen_id"])

		definitions = []
		for col in columns:
			quoted_col = _quote_identifier(col)
			if col == "table_autogen_id":
				definitions.append(f"{quoted_col} BIGINT PRIMARY KEY")
			else:
				definitions.append(f"{quoted_col} TEXT")

		query = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(definitions)})"
		logger.info(f"Executing CREATE TABLE: {query}")
		cursor.execute(query)

	def _execute_alter(self, cursor, command: Dict[str, Any]) -> None:
		table_name = _quote_identifier(command["table_name"])
		table_name_raw = command["table_name"]
		column_name = command.get("column_name")
		if not column_name:
			return

		quoted_column = _quote_identifier(column_name)
		old_type = command.get("old_type")
		new_type = command.get("new_type")
		column_exists = self._column_exists(cursor, table_name_raw, column_name)

		# Storage migration ALTERs may omit type info; default to TEXT when unknown.
		target_sql_type = _sql_type(new_type)

		if not column_exists:
			query = f"ALTER TABLE {table_name} ADD COLUMN {quoted_column} {target_sql_type}"
		elif old_type is not None or new_type is not None:
			query = f"ALTER TABLE {table_name} MODIFY COLUMN {quoted_column} {target_sql_type}"
		else:
			# Storage-only migration command and column already exists in SQL.
			return

		logger.info(f"Executing ALTER TABLE: {query}")

		cursor.execute(query)

	def _column_exists(self, cursor, table_name: str, column_name: str) -> bool:
		query = (
			"SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS "
			"WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s AND COLUMN_NAME = %s LIMIT 1"
		)
		cursor.execute(query, (table_name, column_name))
		return cursor.fetchone() is not None

	def _execute_insert(self, cursor, command: Dict[str, Any]) -> None:
		if command.get("migration"):
			# Migration placeholders from MapRegister are markers, not executable SQL.
			return

		table_name = _quote_identifier(command["table_name"])
		table_name_raw = command["table_name"]
		columns: List[str] = command.get("columns", [])
		values: List[Any] = command.get("values", [])

		if not columns:
			return

		normalized_values: List[Any] = []
		for value in values:
			if isinstance(value, (dict, list)):
				normalized_values.append(json.dumps(value))
			else:
				normalized_values.append(value)

		# Track primary key if available
		if "table_autogen_id" in columns:
			pk_idx = columns.index("table_autogen_id")
			pk_value = values[pk_idx]
			self.mark_pk_inserted(table_name_raw, pk_value)

		quoted_columns = ", ".join(_quote_identifier(c) for c in columns)
		placeholders = ", ".join(["%s"] * len(columns))
		update_clause = ", ".join(
			f"{_quote_identifier(c)} = VALUES({_quote_identifier(c)})"
			for c in columns
			if c != "table_autogen_id"
		)

		query = f"INSERT INTO {table_name} ({quoted_columns}) VALUES ({placeholders})"
		if update_clause:
			query += f" ON DUPLICATE KEY UPDATE {update_clause}"
		else:
			# Make single-key inserts idempotent across restarts.
			query += " ON DUPLICATE KEY UPDATE table_autogen_id = VALUES(table_autogen_id)"
		logger.info(f"Executing INSERT: {query} with values {normalized_values}")
		cursor.execute(query, normalized_values)

	def _execute_delete(self, cursor, command: Dict[str, Any]) -> None:
		criteria: Dict[str, Any] = command.get("criteria") or {}
		if not criteria:
			logger.warning(f"Skipping DELETE with no criteria: {command}")
			return

		table_name = _quote_identifier(command["table_name"])
		where_parts = []
		values: List[Any] = []
		for column, value in criteria.items():
			where_parts.append(f"{_quote_identifier(column)} = %s")
			if isinstance(value, (dict, list)):
				values.append(json.dumps(value))
			else:
				values.append(value)

		query = f"DELETE FROM {table_name} WHERE {' AND '.join(where_parts)}"
		logger.info(f"Executing DELETE: {query} with values {values}")
		cursor.execute(query, values)

	def _execute_update(self, cursor, command: Dict[str, Any]) -> None:
		criteria: Dict[str, Any] = command.get("criteria") or {}
		set_fields: Dict[str, Any] = command.get("set_fields") or {}

		if not criteria:
			logger.warning(f"Skipping UPDATE with no criteria: {command}")
			return
		if not set_fields:
			logger.warning(f"Skipping UPDATE with no set_fields: {command}")
			return

		table_name = _quote_identifier(command["table_name"])
		set_parts = []
		where_parts = []
		values: List[Any] = []

		for column, value in set_fields.items():
			set_parts.append(f"{_quote_identifier(column)} = %s")
			if isinstance(value, (dict, list)):
				values.append(json.dumps(value))
			else:
				values.append(value)

		for column, value in criteria.items():
			where_parts.append(f"{_quote_identifier(column)} = %s")
			if isinstance(value, (dict, list)):
				values.append(json.dumps(value))
			else:
				values.append(value)

		query = f"UPDATE {table_name} SET {', '.join(set_parts)} WHERE {' AND '.join(where_parts)}"
		logger.info(f"Executing UPDATE: {query} with values {values}")
		cursor.execute(query, values)

