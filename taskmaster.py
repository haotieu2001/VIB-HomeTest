from fastapi import FastAPI, HTTPException, status, Response
from pydantic import BaseModel
from typing import List, Dict
import uuid
import time
import json
from enum import Enum
import threading
import pika
from contextlib import asynccontextmanager

class TaskStatus(str, Enum):
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"

class Task(BaseModel):
    id: str
    status: TaskStatus
    message: str
    dependencies: List[str] = []
    requires_ordering: bool = False

class TaskRequest(BaseModel):
    message: str
    dependencies: List[str] = []
    requires_ordering: bool = False

class TaskResponse(BaseModel):
    id: str

# Storage
tasks: Dict[str, Task] = {} # {task_id: Task} 
dependents: Dict[str, List[str]] = {} # {task_id: [dependent_task_id, ...]}


QUEUES = {
    "regular": "taskmaster.tasks.regular", 
    "ordered": "taskmaster.tasks.ordered"
}

def get_rabbitmq_channel(): 
    connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
    channel = connection.channel()
    for queue in QUEUES.values():
        channel.queue_declare(queue=queue, durable=True)
    return connection, channel

def process_task(task_id: str):
    task = tasks[task_id]
    task.status = TaskStatus.IN_PROGRESS
    print(f"Task {task_id}: processing '{task.message}'")
    time.sleep(25)
    print(f"Task {task_id}: completed")
    task.status = TaskStatus.COMPLETED
    for dep_id in dependents.get(task_id, []):
        if all(tasks[d].status == TaskStatus.COMPLETED for d in tasks[dep_id].dependencies):
            queue_task(dep_id)

def queue_task(task_id: str):
    task = tasks[task_id] 
    if any(tasks[d].status != TaskStatus.COMPLETED for d in task.dependencies):
        return False
    try:
        connection, channel = get_rabbitmq_channel()
        queue = QUEUES["ordered"] if task.requires_ordering else QUEUES["regular"]
        
        channel.basic_publish(
            exchange='',
            routing_key=queue,
            body=json.dumps({"task_id": task_id, "message": task.message}),
            properties=pika.BasicProperties(delivery_mode=2)
        )
        connection.close()
        print(f"Task {task_id} queued")
        return True
    except Exception as e:
        print(f"Queue failed for {task_id}: {e}")
        return False

def worker(queue_name: str, worker_id: str):
    print(f"Worker {worker_id} started")
    
    while True:
        try:
            connection, channel = get_rabbitmq_channel()
                    
            def callback(ch, method, properties, body):
                try:
                    task_data = json.loads(body)
                    task_id = task_data["task_id"]
                    print(f"Worker {worker_id} processing {task_id}")
                    process_task(task_id)
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                except Exception as e: # if an error occurs, mark the task as failed and acknowledge the message
                    print(f"Worker {worker_id} error: {e}")
                    task_data = json.loads(body)
                    task_id = task_data["task_id"]
                    if task_id in tasks:
                        tasks[task_id].status = TaskStatus.FAILED
                    ch.basic_ack(delivery_tag=method.delivery_tag)
            channel.basic_qos(prefetch_count=1)
            channel.basic_consume(queue=queue_name, on_message_callback=callback)
            channel.start_consuming()        
             
        except Exception as e:
            print(f"Worker {worker_id} reconnecting: {e}")
            time.sleep(3)

def start_workers():
    workers = [
        (QUEUES["regular"], "regular-0"),
        (QUEUES["regular"], "regular-1"),
        (QUEUES["ordered"], "ordered") ]
    for queue, worker_id in workers:
        threading.Thread(target=worker, args=(queue, worker_id), daemon=True).start()
    print("Workers started")

@asynccontextmanager
async def lifespan(app: FastAPI):
    start_workers()
    yield

app = FastAPI(title="TaskMaster", lifespan=lifespan)

@app.post("/api/tasks", response_model=TaskResponse, status_code=status.HTTP_202_ACCEPTED)
async def create_task(request: TaskRequest):
    task_id = str(uuid.uuid4())[:8]
    for dep_id in request.dependencies:
        if dep_id not in tasks:
            raise HTTPException(status_code=400, detail=f"Dependency {dep_id} not found") 
    task = Task(
        id=task_id,
        status=TaskStatus.PENDING,
        message=request.message,
        dependencies=request.dependencies,
        requires_ordering=request.requires_ordering
    )
    tasks[task_id] = task
    for dep_id in request.dependencies:
        if dep_id not in dependents:
            dependents[dep_id] = []
        dependents[dep_id].append(task_id)
    queue_task(task_id)
    return TaskResponse(id=task_id)

@app.get("/api/tasks/{task_id}", response_model=Task)
async def get_task(task_id: str):
    if task_id not in tasks:
        raise HTTPException(status_code=404, detail="Task not found")
    return tasks[task_id]

@app.get("/api/tasks")
async def get_all_tasks():
    ndjson_content = "\n".join(task.model_dump_json() for task in tasks.values())
    return Response(content=ndjson_content, media_type="application/x-ndjson")

@app.get("/")
async def root():
    return {"message": "TaskMaster API is running"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)