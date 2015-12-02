import os
from celery import Celery
from ingester import Ingester
from time import sleep
import platform


app = Celery('tasks', broker=os.getenv('QUEUE_BROKER', 'memory://localhost'))
app.conf.update(
    CELERY_TASK_SERIALIZER='json',
    CELERY_ACCEPT_CONTENT=['json'],
    CELERY_TIMEZONE='UTC',
    CELERY_ENABLE_UTC=True,
    CELERYBEAT_SCHEDULE={},
)


@app.task
def do_ingest(path):
    """
    Create a new instance of an Ingester and run it's
    ingest() method on a specific path
    """
    ingester = Ingester(path)
    ingester.ingest()
    #  Metrics
    i = app.control.inspect()
    if i.reserved():
        from opentsdb_python_metrics.metric_wrappers import send_tsdb_metric
        reserved = len(i.reserved()['celery@{}'.format(platform.node())])
        send_tsdb_metric('ingester.queue_length', reserved)
        print('reserved items: {}'.format(reserved))
        sleep(2)  # metrics do not block
        print('task done')


@app.task
def heartbeat():
    print('sending heartbeat')
