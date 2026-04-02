
import asyncio
from collections import deque
from utils.log import logger


async def process_records(q, map_register, update_order, stop_event):
    while not stop_event.is_set() or q:
        if not q:
            await asyncio.sleep(0.05)
            continue

        record = q.popleft()
        if "event" not in record:
            logger.error(f"Record missing 'event' field: {record}")
            continue

        event = record["event"]
        if event == "add":
            map_register.ResolveRequest(record["data"], update_order)
        elif event in {"delete", "remove"}:
            delete_handler = getattr(map_register, "DeleteRequest", None) or getattr(map_register, "RemoveRequest", None)
            if callable(delete_handler):
                delete_handler(record["data"], update_order)
            else:
                logger.warning(f"Delete event received but no delete handler is available: {record}")
        else:
            logger.warning(f"Skipping unsupported event type: {event}")


async def dispatch_updates(update_order, sql_queue, nosql_queue, stop_event):
    while not stop_event.is_set() or update_order:
        if not update_order:
            await asyncio.sleep(0.05)
            continue

        command = update_order.popleft()
        executer = command.get("Executer")
        if executer == "SQL":
            sql_queue.append(command)
        elif executer == "NoSQL":
            nosql_queue.append(command)
        else:
            logger.warning(f"Skipping command with unknown Executer: {command}")


async def process_sql_updates(sql_queue, sql_server, stop_event):
    while not stop_event.is_set() or sql_queue:
        if not sql_queue:
            await asyncio.sleep(0.05)
            continue

        command = sql_queue.popleft()
        logger.info(f"Processing SQL command: {command}")
        try:
            await asyncio.to_thread(sql_server.execute_update_order, [command])
        except Exception as e:
            logger.error(f"Error executing SQL command: {e}")
            stop_event.set()
            break


async def process_nosql_updates(nosql_queue, mongo_server, stop_event):
    while not stop_event.is_set() or nosql_queue:
        if not nosql_queue:
            await asyncio.sleep(0.05)
            continue

        command = nosql_queue.popleft()
        logger.info(f"Processing NoSQL command: {command}")
        try:
            await asyncio.to_thread(mongo_server.execute_update_order, [command])
        except Exception as e:
            logger.error(f"Error executing NoSQL command: {e}")
            stop_event.set()
            break


def build_update_queues():
    return deque(), deque()