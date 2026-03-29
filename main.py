from utils.mapregister import MapRegister
from utils.network import stream_sse_records
from collections import deque
import asyncio
from utils.log import logger
from utils.sql import SQLUpdateOrderExecutor
from utils.scheduler import process_records, process_sql_updates


async def main():
    q = deque()
    updateOrder = deque()
    stop_event = asyncio.Event()
    mapRegister = MapRegister(updateOrder=updateOrder)
    sqlServer = SQLUpdateOrderExecutor(
        host="localhost",
        port=3306,
        user="adapter",
        password="ab123",
        database="adapter"
    )

    stream_task = asyncio.create_task(
        stream_sse_records(100000, q, stop_event=stop_event, max_queue_size=50)
    )
    record_task = asyncio.create_task(
        process_records(q, mapRegister, updateOrder, stop_event)
    )
    sql_task = asyncio.create_task(
        process_sql_updates(updateOrder, sqlServer, stop_event)
    )

    try:
        await stream_task
    finally:
        stop_event.set()
        await asyncio.gather(record_task, sql_task, return_exceptions=True)
        sqlServer.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Stopping stream...")