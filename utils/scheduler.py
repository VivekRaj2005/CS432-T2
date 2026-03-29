
import asyncio
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

        if record["event"] == "add":
            map_register.ResolveRequest(record["data"], update_order)


async def process_sql_updates(update_order, sql_server, stop_event):
    while not stop_event.is_set() or update_order:
        if not update_order:
            await asyncio.sleep(0.05)
            continue

        command = update_order.popleft()
        logger.info(f"Processing update command: {command}")
        try:
            # SQL execution is blocking, so run it in a worker thread.
            await asyncio.to_thread(sql_server.execute_update_order, [command])
        except Exception as e:
            logger.error(f"Error executing update command: {e}")
            stop_event.set()
            break