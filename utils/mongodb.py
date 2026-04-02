from typing import Any, Dict, Iterable, List

from pymongo import MongoClient
from pymongo.collection import Collection

from utils.log import logger


class MongoUpdateOrderExecutor:
	def __init__(
		self,
		database: str,
		host: str = "localhost",
		port: int = 27017,
		username: str = None,
		password: str = None,
		connection_string: str = None,
	):
		if connection_string:
			self._client = MongoClient(connection_string)
		elif username and password:
			self._client = MongoClient(host=host, port=port, username=username, password=password)
		else:
			self._client = MongoClient(host=host, port=port)
		self._db = self._client[database]

	def close(self) -> None:
		self._client.close()

	def execute_update_order(self, update_order: Iterable[Dict[str, Any]]) -> None:
		for command in update_order:
			if command.get("Executer") != "NoSQL":
				continue

			command_type = command.get("type")
			if command_type == "CREATE":
				self._execute_create(command)
			elif command_type == "ALTER":
				self._execute_alter(command)
			elif command_type == "INSERT":
				self._execute_insert(command)
			elif command_type in {"DELETE", "REMOVE"}:
				self._execute_delete(command)

	def _collection(self, table_name: str) -> Collection:
		return self._db[table_name]

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
			# Migration placeholders are markers and are not executable writes.
			return

		table_name = command["table_name"]
		columns: List[str] = command.get("columns", [])
		values: List[Any] = command.get("values", [])

		if not columns or len(columns) != len(values):
			logger.warning(f"Skipping invalid NoSQL INSERT command: {command}")
			return

		document = dict(zip(columns, values))
		if "table_autogen_id" not in document:
			logger.warning(f"NoSQL INSERT missing table_autogen_id; skipping: {command}")
			return

		collection = self._collection(table_name)
		collection.update_one(
			{"table_autogen_id": document["table_autogen_id"]},
			{"$set": document},
			upsert=True,
		)
		logger.info(f"Mongo upsert for {table_name} id={document['table_autogen_id']}")

	def _execute_delete(self, command: Dict[str, Any]) -> None:
		criteria: Dict[str, Any] = command.get("criteria") or {}
		if not criteria:
			logger.warning(f"Skipping NoSQL DELETE with no criteria: {command}")
			return

		table_name = command["table_name"]
		collection = self._collection(table_name)
		result = collection.delete_many(criteria)
		logger.info(f"Mongo delete for {table_name} matched={result.deleted_count} criteria={criteria}")
