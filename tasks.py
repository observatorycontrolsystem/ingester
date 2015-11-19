import os
from celery import Celery
from ingester import Ingester

app = Celery('tasks', broker=os.getenv('CONNECT_STR', 'memory://localhost'))


@app.task
def ingest_pack(pack):
    """
    Create a new instance of an Ingester and run it's
    ingest() method on a specific path
    """
    ingester = Ingester()
    ingester.ingest(pack)
