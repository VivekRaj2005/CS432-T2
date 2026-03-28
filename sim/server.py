from fastapi import FastAPI
from sse_starlette.sse import EventSourceResponse
import asyncio
from sim.simulator import generate_add_req, generate_get_req, generate_change_req, generate_remove_req, DECLARE
import json
import random

app = FastAPI()

@app.get("/") # HTTP endpoint method GET, URL /
async def single_record():
    return generate_add_req()

@app.get("/record/{count}") # HTTP endpoint method GET, URL /record/100 say
async def stream_records(count: int):
    async def event_generator():
        yield {"event":"init", "data": json.dumps(DECLARE)} # declaring unique, global keys
        just_add = count//2
        rest = count - just_add
        for _ in range(just_add): # first 50 pc records add
            await asyncio.sleep(0.01)
            yield generate_add_req()
        for _ in range(rest): # of the next 50 pc
            await asyncio.sleep(0.01)
            choice = random.random()
            if choice <= 0.4: # 40 pc add
                yield generate_add_req()
            elif choice<=0.8: # 40 pc get
                yield generate_get_req()
            elif choice<=0.9: # 10 pc change
                yield generate_change_req()
            else: # 10 pc remove
                yield generate_remove_req()
    return EventSourceResponse(event_generator())
