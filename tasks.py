import os
from celery import Celery
from ingester import Ingester

app = Celery('tasks', broker=os.getenv('QUEUE_BROKER', 'memory://localhost'))


@app.task
def do_ingest(path, api_root, s3_bucket):
    """
    Create a new instance of an Ingester and run it's
    ingest() method on a specific path
    """
    ingester = Ingester(api_root, s3_bucket)
    ingester.ingest(path)
