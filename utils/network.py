import requests
import time
import json
import asyncio
from collections import deque
from utils.log import logger
import httpx

session = requests.Session()
session.headers.update({
    'Cache-Control': 'no-cache, no-store, must-revalidate',
    'Pragma': 'no-cache'
})

def fetch_data():
    bust = int(time.time())
    response = requests.get(f"http://127.0.0.1:8000/?_={bust}", headers={'Cache-Control': 'no-cache'})
    if response.status_code == 200:
        return response.json()
    else:
        response.raise_for_status()


async def stream_sse_records(count: int, queue: deque, 
                            stop_event: asyncio.Event = None,
                            url=None, timeout=None, max_queue_size: int = None, 
                            reconnect_delay: float = 0.1):
    if url is None:
        bust = int(time.time()) # Unique number to prevent caching
        url = f"http://127.0.0.1:8000/record/{count}?_={bust}"

    headers = {
        'Accept': 'text/event-stream',
        'Cache-Control': 'no-cache'
    }
    # Keep reconnecting unless stop_event is set
    while True:
        if stop_event is not None and stop_event.is_set():
            logger.info('Stop event set before connect; exiting stream loop')
            return

        try:
            async with httpx.AsyncClient(timeout=timeout) as client:
                async with client.stream('GET', url, headers=headers) as resp:
                    resp.raise_for_status()
                    current_event = None
                    async for raw_line in resp.aiter_lines():
                        # termination check
                        if stop_event is not None and stop_event.is_set():
                            logger.info('Stop event set, ending SSE stream')
                            break

                        # pause if queue is too large
                        if max_queue_size is not None and len(queue) >= max_queue_size:
                            logger.info('Queue reached max size (%d). Pausing ingestion.', max_queue_size)
                            while (len(queue) >= max_queue_size) and (stop_event is None or not stop_event.is_set()):
                                await asyncio.sleep(0.01)
                            logger.info('Queue below threshold. Resuming ingestion.')

                        if raw_line is None:
                            continue
                        line = raw_line.strip()
                        if line.startswith('event:'):
                            current_event = line[len('event: '):].strip()
                        if line.startswith('data:'):
                            data_str = line[len('data: '):].strip()
                            try:
                                record = json.loads(data_str)
                                if 't_stamp' not in record or record['t_stamp'] is None:
                                    record['t_stamp'] = time.time()
                                rec = {"event":current_event, "data":record}
                                queue.append(rec)
                            except json.JSONDecodeError as exc:
                                logger.warning('Failed to parse JSON from SSE data: %s', exc)
        except Exception as exc:
            logger.exception('SSE stream error, will reconnect after delay: %s', exc)

        # If stop_event was set during the stream, exit now
        if stop_event is not None and stop_event.is_set():
            logger.info('Stop event set after disconnect; exiting')
            return

        logger.info('SSE stream ended, reconnecting after %.2fs', reconnect_delay)
        await asyncio.sleep(reconnect_delay)

# Tests
async def test():
    queue = deque()
    stop_event = asyncio.Event()
    # start streaming in background
    task = asyncio.create_task(stream_sse_records(10, queue, stop_event, max_queue_size=1000))
    printed = 0
    try:
        # print first 10 records as a quick smoke test
        while printed < 10:
            if queue:
                record = queue.popleft()
                print(record)
                printed += 1
            else:
                await asyncio.sleep(0.1)
    finally:
        stop_event.set()
        await task