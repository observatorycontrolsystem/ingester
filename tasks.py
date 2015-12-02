from celery import Celery
from ingester import Ingester
from time import sleep
import platform

app = Celery('tasks')
app.config_from_object('settings')


@app.task(bind=True, max_retries=3)
def do_ingest(self, path):
    """
    Create a new instance of an Ingester and run it's
    ingest() method on a specific path
    """
    try:
        ingester = Ingester(path)
        ingester.ingest()
    except Exception as exc:
        raise self.retry(exc=exc)

    #  Metrics
    i = app.control.inspect()
    if i.reserved() or i.active():
        from opentsdb_python_metrics.metric_wrappers import send_tsdb_metric
        host_string = 'celery@{}'.format(platform.node())
        reserved = len(i.reserved()[host_string])
        active = len(i.active()[host_string]) - 1  # exclude this (done) task
        send_tsdb_metric('ingester.queue_length', reserved + active)
        print('queue size: {}'.format(reserved + active))
        sleep(2)  # metrics do not block


@app.task
def heartbeat():
    print('sending heartbeat')
