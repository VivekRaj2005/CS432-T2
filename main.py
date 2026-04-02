from utils.mapregister import MapRegister
from utils.network import stream_sse_records
from collections import deque
import asyncio
from utils.log import logger
from utils.sql import SQLUpdateOrderExecutor
from utils.mongodb import MongoUpdateOrderExecutor
from utils.scheduler import (
    process_records,
    dispatch_updates,
    process_sql_updates,
    process_nosql_updates,
    build_update_queues,
)
from utils.settings import HOST, PORT, USERNAME, PASSWORD, DB, CONNECTION


async def main():
    q = deque()
    updateOrder = deque()
    sql_queue, nosql_queue = build_update_queues()
    stop_event = asyncio.Event()
    mapRegister = MapRegister(updateOrder=updateOrder)
    sqlServer = SQLUpdateOrderExecutor(
        host=HOST,
        port=int(PORT),
        user=USERNAME,
        password=PASSWORD,
        database=DB
    )
    mongoServer = MongoUpdateOrderExecutor(connection_string=CONNECTION, database=DB)

    stream_task = asyncio.create_task(
        stream_sse_records(100000, q, stop_event=stop_event, max_queue_size=50)
    )
    record_task = asyncio.create_task(
        process_records(q, mapRegister, updateOrder, stop_event)
    )
    dispatch_task = asyncio.create_task(
        dispatch_updates(updateOrder, sql_queue, nosql_queue, stop_event)
    )
    sql_task = asyncio.create_task(
        process_sql_updates(sql_queue, sqlServer, stop_event)
    )
    nosql_task = asyncio.create_task(
        process_nosql_updates(nosql_queue, mongoServer, stop_event)
    )

    try:
        await stream_task
    finally:
        stop_event.set()
        await asyncio.gather(record_task, dispatch_task, sql_task, nosql_task, return_exceptions=True)
        sqlServer.close()
        mongoServer.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Stopping stream...")
    finally:
        quit()