import os
from celery import Celery
from ingester import Ingester
from datetime import timedelta

app = Celery('tasks', broker=os.getenv('QUEUE_BROKER', 'memory://localhost'))
app.conf.update(
    CELERY_TASK_SERIALIZER='json',
    CELERY_ACCEPT_CONTENT=['json'],
    CELERY_TIMEZONE='UTC',
    CELERY_ENABLE_UTC=True,
    CELERYBEAT_SCHEDULE={
        'print_evey_5_seconds': {
            'task': 'tasks.hello',
            'schedule': timedelta(seconds=5)
        },
    }
)


@app.task
def do_ingest(path, api_root, s3_bucket):
    """
    Create a new instance of an Ingester and run it's
    ingest() method on a specific path
    """
    ingester = Ingester(api_root, s3_bucket)
    ingester.ingest(path)


@app.task
def hello():
    print('periodic task!')
