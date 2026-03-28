from utils.network import stream_sse_records
from collections import deque
import asyncio

async def main():
    q = deque()
    stop_event = asyncio.Event()
    task = asyncio.create_task(stream_sse_records(100000, q, stop_event=stop_event, max_queue_size=50))
    try:
        while True:
            if q:
                record = q.popleft()
                print(record)
            else:
                await asyncio.sleep(1)
    except KeyboardInterrupt:
        print("Stopping stream...")
        stop_event.set()
        await task

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        print(f"Error: {e}")