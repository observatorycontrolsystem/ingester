import os
from celery import Celery
from ingester import Ingester
from time import sleep
from opentsdb_python_metrics.metric_wrappers import send_tsdb_metric


app = Celery('tasks', broker=os.getenv('QUEUE_BROKER', 'memory://localhost'))
app.conf.update(
    CELERY_TASK_SERIALIZER='json',
    CELERY_ACCEPT_CONTENT=['json'],
    CELERY_TIMEZONE='UTC',
    CELERY_ENABLE_UTC=True,
    CELERYBEAT_SCHEDULE={},
)


@app.task
def do_ingest(path, api_root, s3_bucket):
    """
    Create a new instance of an Ingester and run it's
    ingest() method on a specific path
    """
    ingester = Ingester(api_root, s3_bucket)
    ingester.ingest(path)
    #  Keep an eye on the queue size
    i = app.control.inspect()
    if i.reserved():
        reserved = len(i.reserved()['celery@cygnus'])
        send_tsdb_metric('ingester.queue_length', reserved)
        sleep(2)  # metrics do not block
        print(reserved)


@app.task
def heartbeat():
    print('sending heartbeat')
