from celery import Celery
from ingester.ingester import Ingester
import platform
import logging
from opentsdb_python_metrics.metric_wrappers import send_tsdb_metric, metric_timer

logger = logging.getLogger('ingester')

app = Celery('tasks')
app.config_from_object('settings')


@app.task(bind=True, max_retries=3)
@metric_timer('ingester', async=False)
def do_ingest(self, path, bucket):
    """
    Create a new instance of an Ingester and run it's
    ingest() method on a specific path
    """
    try:
        ingester = Ingester(path, bucket)
        ingester.ingest()
    except Exception as exc:
        logger.fatal('Exception raised while processing {0}: {1}'.format(path, exc))
        raise self.retry(exc=exc)
    collect_queue_length_metric()


def collect_queue_length_metric():
    i = app.control.inspect()
    if i.reserved() or i.active():
        host_string = 'celery@{}'.format(platform.node())
        reserved = len(i.reserved()[host_string])
        active = len(i.active()[host_string]) - 1  # exclude this (done) task
        send_tsdb_metric('ingester.queue_length', reserved + active, async=False)
