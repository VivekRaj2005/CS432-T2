from typing import Any, Dict, Iterable, List, Set, Optional
from collections import defaultdict

from pymongo import MongoClient
from pymongo.collection import Collection

from utils.log import mongodb_logger as logger


class MongoUpdateOrderExecutor:
	def __init__(
		self,
		database: str,
		host: str = "localhost",
		port: int = 27017,
		username: str = None,
		password: str = None,
		connection_string: str = None,
		schema_manager=None,
	):
		if connection_string:
			logger.info(f"Connecting to MongoDB using connection string")
			self._client = MongoClient(connection_string)
		elif username and password:
			logger.info(f"Connecting to MongoDB: {username}@{host}:{port}")
			self._client = MongoClient(host=host, port=port, username=username, password=password)
		else:
			logger.info(f"Connecting to MongoDB: {host}:{port}")
			self._client = MongoClient(host=host, port=port)
		self._db = self._client[database]
		logger.info(f"Connected to MongoDB database: {database}")
		# Optional schema manager for schema-aware operations
		self.schema_manager = schema_manager
		# Track seen PKs per collection: collection_name -> set of PK values
		self._seen_pks: Dict[str, Set[Any]] = defaultdict(set)

	def mark_pk_inserted(self, collection_name: str, pk_value: Any) -> None:
		"""Track that a primary key has been inserted."""
		self._seen_pks[collection_name].add(pk_value)

	def is_first_insert(self, collection_name: str, pk_value: Any) -> bool:
		"""Check if this PK value has been seen before."""
		return pk_value not in self._seen_pks[collection_name]

	def unmark_pk(self, collection_name: str, pk_value: Any) -> None:
		"""Remove a PK from tracking (e.g., after DELETE)."""
		self._seen_pks[collection_name].discard(pk_value)

	def get_schema(self) -> Optional[Dict]:
		"""Get the current schema from schema manager."""
		if self.schema_manager:
			return self.schema_manager.get_schema()
		return None

	def close(self) -> None:
		self._client.close()

	def execute_update_order(self, update_order: Iterable[Dict[str, Any]]) -> None:
		logger.info("Starting MongoDB update order processing...")
		command_count = 0
		for command in update_order:
			if command.get("Executer") != "NoSQL":
				continue

			command_type = command.get("type")
			table_name = command.get("table_name", "unknown")
			command_count += 1
			logger.debug(f"Executing MongoDB {command_type} on {table_name} (cmd #{command_count})")
			
			if command_type == "CREATE":
				self._execute_create(command)
			elif command_type == "ALTER":
				self._execute_alter(command)
			elif command_type == "INSERT":
				self._execute_insert(command)
			elif command_type == "UPDATE":
				self._execute_update(command)
			elif command_type in {"DELETE", "REMOVE"}:
				self._execute_delete(command)
		
		logger.info(f"MongoDB update order complete. Executed {command_count} commands")

	def _collection(self, table_name: str) -> Collection:
		return self._db[table_name]

	def fetch_column_snapshot(self, table_name: str, column_name: str) -> List[Dict[str, Any]]:
		collection = self._collection(table_name)
		cursor = collection.find(
			{
				"table_autogen_id": {"$exists": True},
				column_name: {"$exists": True},
			},
			{"_id": 0, "table_autogen_id": 1, column_name: 1},
		)
		return list(cursor)

	def fetch_records(
		self,
		table_name: str,
		criteria: Optional[Dict[str, Any]] = None,
		fields: Optional[List[str]] = None,
		limit: int = 100,
	) -> List[Dict[str, Any]]:
		criteria = criteria or {}
		limit = max(1, min(limit, 1000))
		collection = self._collection(table_name)
		projection = {"_id": 0}
		if fields:
			projection = {"_id": 0}
			for field in fields:
				projection[field] = 1

		cursor = collection.find(criteria, projection).limit(limit)
		return list(cursor)

	def remove_column_for_ids(self, table_name: str, column_name: str, ids: List[Any]) -> int:
		if not ids:
			return 0

		collection = self._collection(table_name)
		result = collection.update_many(
			{"table_autogen_id": {"$in": ids}},
			{"$unset": {column_name: ""}},
		)
		logger.info(
			"Removed migrated field '%s' from %d Mongo documents in %s",
			column_name,
			result.modified_count,
			table_name,
		)
		return result.modified_count

	def _execute_create(self, command: Dict[str, Any]) -> None:
		table_name = command["table_name"]
		collection = self._collection(table_name)

		# Ensure collection exists and enforce unique cross-db identity key.
		collection.create_index("table_autogen_id", unique=True)
		logger.info(f"Ensured Mongo collection/index for: {table_name}")

	def _execute_alter(self, command: Dict[str, Any]) -> None:
		# MongoDB is schema-flexible; ALTER is metadata-only for NoSQL path.
		logger.info(f"NoSQL ALTER is a no-op in MongoDB: {command}")

	def _execute_insert(self, command: Dict[str, Any]) -> None:
		if command.get("migration"):
			self._execute_migration_insert(command)
			return

		table_name = command["table_name"]
		columns: List[str] = command.get("columns", [])
		values: List[Any] = command.get("values", [])

		if not columns or len(columns) != len(values):
			logger.warning(f"Skipping invalid NoSQL INSERT command: {command}")
			return

		document = dict(zip(columns, values))
		raw_record = command.get("raw_record")
		if isinstance(raw_record, dict):
			for key, value in raw_record.items():
				if key == "table_autogen_id":
					continue
				document.setdefault(key, value)
		if "table_autogen_id" not in document:
			logger.warning(f"NoSQL INSERT missing table_autogen_id; skipping: {command}")
			return

		# Track the PK
		pk_value = document["table_autogen_id"]
		self.mark_pk_inserted(table_name, pk_value)

		collection = self._collection(table_name)
		collection.update_one(
			{"table_autogen_id": pk_value},
			{"$set": document},
			upsert=True,
		)
		logger.info(f"Mongo upsert for {table_name} id={pk_value}")

	def _execute_delete(self, command: Dict[str, Any]) -> None:
		criteria: Dict[str, Any] = command.get("criteria") or {}
		if not criteria:
			logger.warning(f"Skipping NoSQL DELETE with no criteria: {command}")
			return

		table_name = command["table_name"]
		# If deleting by table_autogen_id, unmark it
		if "table_autogen_id" in criteria:
			self.unmark_pk(table_name, criteria["table_autogen_id"])
		
		collection = self._collection(table_name)
		result = collection.delete_many(criteria)
		logger.info(f"Mongo delete for {table_name} matched={result.deleted_count} criteria={criteria}")

	def _execute_update(self, command: Dict[str, Any]) -> None:
		criteria: Dict[str, Any] = command.get("criteria") or {}
		set_fields: Dict[str, Any] = command.get("set_fields") or {}
		if not criteria:
			logger.warning(f"Skipping NoSQL UPDATE with no criteria: {command}")
			return
		if not set_fields:
			logger.warning(f"Skipping NoSQL UPDATE with no set_fields: {command}")
			return

		table_name = command["table_name"]
		collection = self._collection(table_name)
		result = collection.update_many(criteria, {"$set": set_fields})
		logger.info(
			f"Mongo update for {table_name} matched={result.matched_count} modified={result.modified_count} criteria={criteria}"
		)

	def _execute_migration_insert(self, command: Dict[str, Any]) -> None:
		table_name = command.get("table_name")
		column_name = command.get("migration_column")
		transfer_rows = command.get("transfer_rows") or []

		if not table_name or not column_name:
			logger.warning(f"Migration INSERT missing table_name or migration_column; skipping: {command}")
			return

		if not transfer_rows:
			logger.info(
				"No historical SQL rows found for migration of %s.%s",
				table_name,
				column_name,
			)
			return

		collection = self._collection(table_name)
		batch = []
		for row in transfer_rows:
			pk_value = row.get("table_autogen_id")
			if pk_value is None:
				continue

			value = row.get(column_name)
			if value is None:
				continue

			document = {"table_autogen_id": pk_value, column_name: value}
			batch.append(document)

		if not batch:
			logger.info(
				"Migration rows present but all values were NULL for %s.%s; skipping Mongo transfer",
				table_name,
				column_name,
			)
			return

		collection.create_index("table_autogen_id", unique=True)
		logger.info(
			"Migrating %d historical rows from SQL to Mongo for %s.%s",
			len(batch),
			table_name,
			column_name,
		)
		for document in batch:
			collection.update_one(
				{"table_autogen_id": document["table_autogen_id"]},
				{"$set": document},
				upsert=True,
			)
