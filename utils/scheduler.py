
import asyncio
import re
from collections import deque, defaultdict
from typing import Optional
from utils.log import scheduler_logger as logger


class CrudOperationGenerator:
    """
    Generates normalized CRUD operations from raw events.
    Supports schema-aware operation generation and PK tracking.
    """
    
    def __init__(self, schema_manager=None):
        """
        Initialize CRUD operation generator.
        
        Args:
            schema_manager: Optional SchemaManager for schema-aware operations
        """
        self.schema_manager = schema_manager
        # Track seen PKs per table
        self._seen_pks = defaultdict(set)
        logger.info("CrudOperationGenerator initialized")
    
    def generate_operation(self, event: str, data: dict, global_key: Optional[str] = None) -> dict:
        """
        Generate a normalized operation from a CRUD event.
        
        Args:
            event: Event type (add, update/change, delete/remove, get)
            data: Event data payload
            global_key: Optional global identifier field
            
        Returns:
            Normalized operation dict
        """
        event = event.lower().strip()
        logger.debug(f"Generating operation for event: {event}")
        
        if event in ("add", "create"):
            op = self._gen_create(data, global_key)
            logger.info(f"Created INSERT operation for record {data.get(global_key, 'unknown')}")
            return op
        elif event in ("update", "change"):
            op = self._gen_update(data, global_key)
            logger.info(f"Created UPDATE operation for record {data.get(global_key, 'unknown')}")
            return op
        elif event in ("delete", "remove"):
            op = self._gen_delete(data, global_key)
            logger.info(f"Created DELETE operation for record {data.get(global_key, 'unknown')}")
            return op
        elif event == "get":
            op = self._gen_select(data, global_key)
            logger.info(f"Created SELECT operation for record {data.get(global_key, 'unknown')}")
            return op
        else:
            logger.warning(f"Unknown event type: {event}")
            return {}
    
    def _gen_create(self, data: dict, global_key: Optional[str]) -> dict:
        """Generate INSERT operation."""
        return {
            "type": "INSERT",
            "data": data,
            "global_key": global_key,
            "global_key_value": data.get(global_key) if global_key else None
        }
    
    def _gen_update(self, data: dict, global_key: Optional[str]) -> dict:
        """Generate UPDATE operation."""
        # Try to infer criteria from identifier fields
        criteria = {}
        for key in data:
            if key.endswith("_id") or key in {"dept_name", "record_id"}:
                criteria[key] = data[key]
        
        return {
            "type": "UPDATE",
            "data": data,
            "criteria": criteria,
            "global_key": global_key,
            "global_key_value": data.get(global_key) if global_key else None
        }
    
    def _gen_delete(self, data: dict, global_key: Optional[str]) -> dict:
        """Generate DELETE operation."""
        criteria = {}
        for key in data:
            if key.endswith("_id") or key in {"dept_name", "record_id"}:
                criteria[key] = data[key]
        
        return {
            "type": "DELETE",
            "criteria": criteria,
            "global_key": global_key,
            "global_key_value": data.get(global_key) if global_key else None
        }
    
    def _gen_select(self, data: dict, global_key: Optional[str]) -> dict:
        """Generate SELECT operation."""
        criteria = {}
        for key in data:
            if key.endswith("_id") or key in {"dept_name", "record_id"}:
                criteria[key] = data[key]
        
        columns = data.get("COLUMNS", [])  # Hint for specific columns
        
        return {
            "type": "SELECT",
            "criteria": criteria,
            "columns": columns,
            "global_key": global_key,
            "global_key_value": data.get(global_key) if global_key else None
        }


async def process_records(q, map_register, update_order, stop_event):
    logger.info("Starting record processing...")
    record_count = 0
    while not stop_event.is_set() or q:
        if not q:
            await asyncio.sleep(0.05)
            continue

        record = q.popleft()
        record_count += 1
        if "event" not in record:
            logger.error(f"Record missing 'event' field: {record}")
            continue

        event = record["event"]
        logger.debug(f"Processing record {record_count} with event '{event}'")
        
        if event == "add":
            map_register.ResolveRequest(record["data"], update_order)
            logger.info(f"Added record {record_count} to map_register")
        elif event in {"update", "change"}:
            update_handler = getattr(map_register, "UpdateRequest", None) or getattr(map_register, "ChangeRequest", None)
            if callable(update_handler):
                update_handler(record["data"], update_order)
                logger.info(f"Updated record {record_count} via map_register")
            else:
                logger.warning(f"Update event received but no update handler is available: {record}")
        elif event in {"delete", "remove"}:
            delete_handler = getattr(map_register, "DeleteRequest", None) or getattr(map_register, "RemoveRequest", None)
            if callable(delete_handler):
                delete_handler(record["data"], update_order)
                logger.info(f"Deleted record {record_count} via map_register")
            else:
                logger.warning(f"Delete event received but no delete handler is available: {record}")
        else:
            logger.warning(f"Skipping unsupported event type: {event}")
    
    logger.info(f"Record processing complete. Processed {record_count} records total")


async def dispatch_updates(update_order, sql_queue, nosql_queue, stop_event):
    logger.info("Starting update dispatcher...")
    dispatch_count = 0
    
    while not stop_event.is_set() or update_order:
        if not update_order:
            await asyncio.sleep(0.05)
            continue

        command = update_order.popleft()
        dispatch_count += 1
        executer = command.get("Executer")
        cmd_type = command.get("type")
        
        if executer == "SQL":
            sql_queue.append(command)
            logger.debug(f"Dispatched {cmd_type} command to SQL queue (total: {dispatch_count})")
        elif executer == "NoSQL":
            nosql_queue.append(command)
            logger.debug(f"Dispatched {cmd_type} command to NoSQL queue (total: {dispatch_count})")
        else:
            logger.warning(f"Skipping command with unknown Executer: {command}")
    
    logger.info(f"Update dispatcher complete. Dispatched {dispatch_count} commands total")


async def process_sql_updates(sql_queue, sql_server, mongo_server, stop_event):
    logger.info("Starting SQL update processor...")
    sql_count = 0
    
    while not stop_event.is_set() or sql_queue:
        if not sql_queue:
            await asyncio.sleep(0.05)
            continue

        command = sql_queue.popleft()
        migrated_ids = []
        migration_column = None
        if command.get("migration") and command.get("type") == "INSERT":
            values = command.get("values") or []
            if len(values) > 1 and isinstance(values[1], str):
                match = re.match(r"<COPY:(SQL|NoSQL)->(SQL|NoSQL):([^>]+)>", values[1])
                if match and match.group(1) == "NoSQL" and match.group(2) == "SQL":
                    migration_column = match.group(3)

            if migration_column:
                source_rows = await asyncio.to_thread(
                    mongo_server.fetch_column_snapshot,
                    command.get("table_name"),
                    migration_column,
                )
                migrated_ids = [row.get("table_autogen_id") for row in source_rows if row.get("table_autogen_id") is not None]
                command = {
                    **command,
                    "migration_column": migration_column,
                    "transfer_rows": source_rows,
                }

        sql_count += 1
        cmd_type = command.get("type")
        table = command.get("table_name")
        
        logger.info(f"Processing SQL {cmd_type} on {table} (count: {sql_count})")
        try:
            await asyncio.to_thread(sql_server.execute_update_order, [command])
            if migration_column and migrated_ids:
                await asyncio.to_thread(
                    mongo_server.remove_column_for_ids,
                    command.get("table_name"),
                    migration_column,
                    migrated_ids,
                )
            logger.debug(f"SQL {cmd_type} executed successfully")
        except Exception as e:
            logger.error(f"Error executing SQL {cmd_type} command: {e}", exc_info=True)
            stop_event.set()
            break
    
    logger.info(f"SQL update processor complete. Processed {sql_count} SQL commands total")
    


async def process_nosql_updates(nosql_queue, sql_server, mongo_server, stop_event):
    logger.info("Starting NoSQL update processor...")
    nosql_count = 0
    
    while not stop_event.is_set() or nosql_queue:
        if not nosql_queue:
            await asyncio.sleep(0.05)
            continue

        command = nosql_queue.popleft()
        migrated_ids = []
        migration_column = None
        if command.get("migration") and command.get("type") == "INSERT":
            values = command.get("values") or []
            if len(values) > 1 and isinstance(values[1], str):
                match = re.match(r"<COPY:(SQL|NoSQL)->(SQL|NoSQL):([^>]+)>", values[1])
                if match and match.group(1) == "SQL" and match.group(2) == "NoSQL":
                    migration_column = match.group(3)

            if migration_column:
                source_rows = await asyncio.to_thread(
                    sql_server.fetch_column_snapshot,
                    command.get("table_name"),
                    migration_column,
                )
                filtered_rows = []
                for row in source_rows:
                    value = row.get(migration_column)
                    if value is None:
                        continue
                    filtered_rows.append(row)

                migrated_ids = [row.get("table_autogen_id") for row in filtered_rows if row.get("table_autogen_id") is not None]
                command = {
                    **command,
                    "migration_column": migration_column,
                    "transfer_rows": filtered_rows,
                }

        nosql_count += 1
        cmd_type = command.get("type")
        table = command.get("table_name")
        
        logger.info(f"Processing NoSQL {cmd_type} on {table} (count: {nosql_count})")
        try:
            await asyncio.to_thread(mongo_server.execute_update_order, [command])
            logger.debug(f"NoSQL {cmd_type} executed successfully")
        except Exception as e:
            logger.error(f"Error executing NoSQL {cmd_type} command: {e}", exc_info=True)
            stop_event.set()
            break
    
    logger.info(f"NoSQL update processor complete. Processed {nosql_count} NoSQL commands total")


def build_update_queues():
    return deque(), deque()