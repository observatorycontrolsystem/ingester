from celery import Celery
from ingester.ingester import Ingester
from ingester.exceptions import RetryError, DoNotRetryError, BackoffRetryError
import platform
import logging
from opentsdb_python_metrics.metric_wrappers import send_tsdb_metric, metric_timer
logger = logging.getLogger('ingester')

app = Celery('tasks')
app.config_from_object('settings')


@app.task(bind=True, max_retries=3, default_retry_delay=3 * 60)
@metric_timer('ingester', async=False)
def do_ingest(self, path, bucket):
    """
    Create a new instance of an Ingester and run it's
    ingest() method on a specific path
    """
    log_tags = {'tags': {'path': path, 'attempt': self.request.retries + 1}}
    try:
        ingester = Ingester(path, bucket)
        ingester.ingest()
    except DoNotRetryError as exc:
        logger.fatal('Exception occured: {0}. Aborting.'.format(exc), extra=log_tags)
        raise exc
    except (RetryError, BackoffRetryError) as exc:
        if self.request.retries < self.max_retries:
            logger.warn(
                'Exception occured: {0}. Will retry'.format(exc),
                extra=log_tags
            )
            if exc.__class__ == BackoffRetryError:
                raise self.retry(exc=exc, countdown=5 ** self.request.retries)
            else:
                raise self.retry(exc=exc)
        else:
            logger.fatal(
                'Excpetion occured: {0}. No more retries. Aborting.'.format(exc),
                extra=log_tags
            )
            raise exc
    collect_queue_length_metric()


def collect_queue_length_metric():
    i = app.control.inspect()
    if i.reserved() or i.active():
        host_string = 'celery@{}'.format(platform.node())
        reserved = len(i.reserved()[host_string])
        active = len(i.active()[host_string]) - 1  # exclude this (done) task
        send_tsdb_metric('ingester.queue_length', reserved + active, async=False)
