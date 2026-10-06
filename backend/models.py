from typing import List, Optional
from pydantic import BaseModel, Field
from datetime import datetime
import uuid

class TaskRequest(BaseModel):
    router_id: str = Field(..., description="Target router name: Router1, Router2, or all")
    commands: List[str] = Field(..., description="List of CLI commands to execute")
    task_id: Optional[str] = Field(default=None, description="Optional custom task ID")

class TaskResponse(BaseModel):
    task_id: str
    router_id: str
    status: str = "QUEUED"
    message: str = "Task submitted to Kafka queue"
    timestamp: str

class TaskResult(BaseModel):
    task_id: str
    router_id: str
    status: str  # SUCCESS, FAILED
    output: str
    execution_time_ms: float
    timestamp: str
    error: Optional[str] = None
    is_mock: Optional[bool] = False

class RouterConfig(BaseModel):
    id: str
    name: str
    host: str
    port: int = 22
    username: str
    description: Optional[str] = ""
    status: Optional[str] = "configured"
