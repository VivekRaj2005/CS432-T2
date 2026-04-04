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
import os
import uvicorn
from server import app


async def main():
    q = deque()
    updateOrder = deque()
    sql_queue, nosql_queue = build_update_queues()
    stop_event = asyncio.Event()
    mapRegister = MapRegister(updateOrder=updateOrder)
    mapRegister.Load("map_register.pkl")
    app.state.map_register = mapRegister
    app.state.ingest_queue = q
    api_server = uvicorn.Server(
        uvicorn.Config(
            app,
            host=os.getenv("API_HOST", "127.0.0.1"),
            port=int(os.getenv("API_PORT", "8000")),
            reload=False,
            log_level="info",
        )
    )
    sqlServer = SQLUpdateOrderExecutor(
        host=HOST,
        port=int(PORT),
        user=USERNAME,
        password=PASSWORD,
        database=DB
    )
    mongoServer = MongoUpdateOrderExecutor(connection_string=CONNECTION, database=DB)
    app.state.sql_server = sqlServer
    app.state.mongo_server = mongoServer

    api_task = asyncio.create_task(api_server.serve())
    record_task = asyncio.create_task(
        process_records(q, mapRegister, updateOrder, stop_event)
    )
    dispatch_task = asyncio.create_task(
        dispatch_updates(updateOrder, sql_queue, nosql_queue, stop_event)
    )
    sql_task = asyncio.create_task(
        process_sql_updates(sql_queue, sqlServer, mongoServer, stop_event)
    )
    nosql_task = asyncio.create_task(
        process_nosql_updates(nosql_queue, mongoServer, stop_event)
    )

    try:
        await api_task
    finally:
        mapRegister.Save("map_register.pkl")
        stop_event.set()
        api_server.should_exit = True
        await asyncio.gather(
            record_task,
            dispatch_task,
            sql_task,
            nosql_task,
            api_task,
            return_exceptions=True,
        )
        sqlServer.close()
        mongoServer.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Stopping stream...")
    finally:
        quit()