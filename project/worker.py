import os
import time

from celery import Celery
from celery import Task
from pytz import timezone

celery = Celery(__name__)
celery.conf.broker_url = os.environ.get("CELERY_BROKER_URL", "redis://localhost:6379")
celery.conf.result_backend = os.environ.get("CELERY_RESULT_BACKEND", "redis://localhost:6379")
celery.conf.timezone = 'Turkey'


@celery.task(name="create_task")
def create_task(duration):
    time.sleep(int(duration))    
    return f"worked for {int(duration)} secs task id "

