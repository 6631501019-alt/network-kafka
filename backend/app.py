import asyncio
import json
import logging
import os
import sys
import threading
import time
import uuid
from datetime import datetime
from typing import Dict, List, Optional

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from kafka import KafkaConsumer, KafkaProducer

from config import (
    CONSUMER_GROUP_BACKEND,
    KAFKA_BOOTSTRAP_SERVERS,
    TOPIC_RESULTS,
    TOPIC_TASKS,
)
from models import RouterConfig, TaskRequest, TaskResponse, TaskResult

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("backend")

app = FastAPI(title="Network Automation & Telemetry Backend", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# In-memory storage for tasks and results
tasks_db: Dict[str, dict] = {}

# Active WebSocket connections
active_connections: List[WebSocket] = []

kafka_producer: Optional[KafkaProducer] = None

def get_producer() -> KafkaProducer:
    global kafka_producer
    if kafka_producer is None:
        try:
            kafka_producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
            )
            logger.info("Kafka Producer initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize Kafka Producer: {e}")
            raise HTTPException(status_code=503, detail=f"Kafka unavailable: {e}")
    return kafka_producer

async def broadcast_ws(message: dict):
    disconnected = []
    for ws in active_connections:
        try:
            await ws.send_json(message)
        except Exception:
            disconnected.append(ws)
    for ws in disconnected:
        if ws in active_connections:
            active_connections.remove(ws)

def results_consumer_loop(loop: asyncio.AbstractEventLoop):
    """
    Background worker that listens to 'router-results' from Kafka
    and updates the tasks_db, then triggers WebSocket broadcast.
    """
    logger.info(f"Starting Results Consumer on topic '{TOPIC_RESULTS}'...")
    while True:
        try:
            consumer = KafkaConsumer(
                TOPIC_RESULTS,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                group_id=f"{CONSUMER_GROUP_BACKEND}-{uuid.uuid4().hex[:6]}",
                auto_offset_reset="latest",
                enable_auto_commit=True,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            )
            logger.info("Connected to Kafka topic 'router-results'")
            for msg in consumer:
                data = msg.value
                task_id = data.get("task_id")
                if task_id in tasks_db:
                    # Update existing task
                    tasks_db[task_id].update({
                        "status": data.get("status", "SUCCESS"),
                        "output": data.get("output", ""),
                        "execution_time_ms": data.get("execution_time_ms", 0),
                        "is_mock": data.get("is_mock", False),
                        "completed_at": data.get("timestamp", datetime.utcnow().isoformat() + "Z"),
                        "error": data.get("error")
                    })
                    logger.info(f"Task [{task_id}] updated with status: {data.get('status')}")
                    
                    # Schedule broadcast in the asyncio event loop
                    asyncio.run_coroutine_threadsafe(
                        broadcast_ws({
                            "type": "TASK_COMPLETED",
                            "data": tasks_db[task_id]
                        }),
                        loop
                    )
        except Exception as e:
            logger.error(f"Error in results_consumer_loop: {e}. Retrying in 3s...")
            time.sleep(3)

@app.on_event("startup")
async def startup_event():
    logger.info("FastAPI Backend starting up...")
    loop = asyncio.get_running_loop()
    # Start Kafka result consumer in a background thread
    t = threading.Thread(target=results_consumer_loop, args=(loop,), daemon=True)
    t.start()

@app.get("/api/health")
def health_check():
    kafka_ok = False
    try:
        p = get_producer()
        kafka_ok = p.partitions_for(TOPIC_TASKS) is not None
    except Exception:
        pass
    return {
        "status": "online",
        "kafka_connected": kafka_ok,
        "bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS,
        "timestamp": datetime.utcnow().isoformat() + "Z"
    }

@app.get("/api/routers")
def get_routers():
    # Return inventory details
    # We can read from worker inventory or default
    return [
        {
            "id": "Router1",
            "name": "Core-Router-1",
            "host": os.getenv("ROUTER1_HOST", "192.168.122.101"),
            "port": int(os.getenv("ROUTER1_PORT", 22)),
            "username": os.getenv("ROUTER1_USER", "admin"),
            "description": "GNS3 Router 1 (Gateway / Core)"
        },
        {
            "id": "Router2",
            "name": "Distribution-Router-2",
            "host": os.getenv("ROUTER2_HOST", "192.168.122.102"),
            "port": int(os.getenv("ROUTER2_PORT", 22)),
            "username": os.getenv("ROUTER2_USER", "admin"),
            "description": "GNS3 Router 2 (Edge / Distribution)"
        }
    ]

@app.post("/api/tasks", response_model=TaskResponse)
async def submit_task(req: TaskRequest):
    producer = get_producer()
    task_id = req.task_id or f"task-{uuid.uuid4().hex[:8]}"
    now_str = datetime.utcnow().isoformat() + "Z"

    task_payload = {
        "task_id": task_id,
        "router_id": req.router_id,
        "commands": req.commands,
        "created_at": now_str,
    }

    # Store in memory
    tasks_db[task_id] = {
        "task_id": task_id,
        "router_id": req.router_id,
        "commands": req.commands,
        "status": "QUEUED",
        "output": "",
        "created_at": now_str,
        "completed_at": None,
        "execution_time_ms": 0,
        "is_mock": False
    }

    try:
        producer.send(TOPIC_TASKS, value=task_payload)
        producer.flush()
        logger.info(f"Task [{task_id}] produced to Kafka topic '{TOPIC_TASKS}'")
    except Exception as e:
        tasks_db[task_id]["status"] = "ERROR"
        tasks_db[task_id]["error"] = str(e)
        logger.error(f"Failed to produce task to Kafka: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to publish to Kafka: {e}")

    await broadcast_ws({
        "type": "TASK_CREATED",
        "data": tasks_db[task_id]
    })

    return TaskResponse(
        task_id=task_id,
        router_id=req.router_id,
        status="QUEUED",
        message="Task submitted to Kafka queue",
        timestamp=now_str
    )

@app.get("/api/tasks")
def list_tasks():
    # Return last 50 tasks in reverse chronological order
    task_list = list(tasks_db.values())
    task_list.sort(key=lambda x: x["created_at"], reverse=True)
    return task_list[:50]

@app.get("/api/tasks/{task_id}")
def get_task(task_id: str):
    if task_id not in tasks_db:
        raise HTTPException(status_code=404, detail="Task not found")
    return tasks_db[task_id]

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    active_connections.append(websocket)
    logger.info(f"WebSocket client connected. Total clients: {len(active_connections)}")
    try:
        while True:
            # Keep connection alive
            await websocket.receive_text()
    except WebSocketDisconnect:
        if websocket in active_connections:
            active_connections.remove(websocket)
        logger.info("WebSocket client disconnected")

# Serve Frontend static assets
frontend_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "frontend"))
if os.path.exists(frontend_dir):
    app.mount("/static", StaticFiles(directory=frontend_dir), name="static")

@app.get("/")
def serve_index():
    index_file = os.path.join(frontend_dir, "index.html")
    if os.path.exists(index_file):
        return FileResponse(index_file)
    return {"message": "Frontend not found, please check frontend/index.html"}
