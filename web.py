from src.task import PdfScrapingTask
from helpers import query, log

from decide_ai_service_base.sparql_config import prefixed_log
from decide_ai_service_base.util import fail_busy_and_scheduled_tasks, wait_for_triplestore, process_open_tasks
from fastapi import APIRouter, BackgroundTasks
from pydantic import BaseModel


@app.on_event("startup")
async def startup_event():
    wait_for_triplestore()
    # on startup fail existing busy tasks
    fail_busy_and_scheduled_tasks()
    # on startup also immediately start scheduled tasks
    process_open_tasks()


router = APIRouter()


class NotificationResponse(BaseModel):
    status: str
    message: str


@router.post("/delta", status_code=202)
def delta(background_tasks: BackgroundTasks) -> NotificationResponse:
    # naively start processing on any incoming delta
    prefixed_log("Received delta notification with")
    background_tasks.add_task(process_open_tasks)
    return NotificationResponse(
        status="accepted",
        message="Processing started",
    )
