from traceback import print_tb
from fastapi import Body, FastAPI, Form, Request
from fastapi.responses import JSONResponse
from worker import create_task
from celery.result import AsyncResult
import aioredis
redis_cache= aioredis.Redis(host='redis', port=6379)

app = FastAPI()

#update this value for creating key space in redis for current app
task_prefix = "MyTasks"
#update this according to user id pattern
user_id_pattern = "[a-zA-Z0-9]*"


@app.post("/tasks", status_code=201)
async def run_task(payload = Body(...)):
    duration = payload["duration"]
    userid = payload["userid"]  
    task = create_task.delay(duration=int(duration))
    redis = await aioredis.Redis(host='redis', port=6379)   
    task_name = f'{task_prefix}.{userid}.{task.id}'
    await  redis.set(task_name, task.id)
    await redis.close()    
    return JSONResponse({"task_id": task.id})


@app.get("/status/")
async def get_status_all():
    redis = aioredis.Redis(host='redis', port=6379)    
    it = redis.scan_iter(match=f'{task_prefix}.*')     
    import re    
    pref = f"{task_prefix}."    
    task_and_user_ids = [(v.decode().split(pref)[1].split('.')[0], re.sub(f'^{task_prefix}.{user_id_pattern}.', '', v.decode())) async for v in it]      
    task_status_list = []
    for user_id, task_id in task_and_user_ids:        
        task_item = {            
            "task_user_id": user_id,
            "task_task_id": task_id,
            "task_status": AsyncResult(task_id).status,
            "task_result": AsyncResult(task_id).result,            
        }
        task_status_list.append(task_item)    
    return task_status_list        
        
    
@app.get("/status/{user_id}")
async def get_status_for_user(user_id:str):
    redis = aioredis.Redis(host='redis', port=6379)    
    it = redis.scan_iter(match=f'{task_prefix}.{user_id}.*')     
    task_ids = [v.decode().replace(f'{task_prefix}.{user_id}.', "") async for v in it]        
    await redis.close()
    task_status_list = []
    for task_id in task_ids:   
        task_item = {            
            "task_task_id": task_id,
            "task_status": AsyncResult(task_id).status,
            "task_result": AsyncResult(task_id).result,            
        }
        task_status_list.append(task_item)    
    return task_status_list

    