from celery import Celery
from ingester.ingester import Ingester
from ingester.exceptions import RetryError, DoNotRetryError, BackoffRetryError
import platform
import logging
import os
from opentsdb_python_metrics.metric_wrappers import send_tsdb_metric, metric_timer

logger = logging.getLogger('ingester')
app = Celery('tasks')
app.config_from_object('settings')


def task_log(task):
    path = task.request.args[0] or ''
    return {
        'tags': {
            'filename': os.path.basename(path),
            'path': path,
            'attempt': task.request.retries + 1
        }
    }


@app.task(bind=True, max_retries=3, default_retry_delay=3 * 60)
@metric_timer('ingester', async=False)
def do_ingest(self, path, bucket, api_root, auth_token, required_headers, blacklist_headers):
    """
    Create a new instance of an Ingester and run it's
    ingest() method on a specific path
    """
    logger.info('Starting ingest', extra=task_log(self))
    try:
        ingester = Ingester(path, bucket, api_root, auth_token, required_headers, blacklist_headers)
        ingester.ingest()
    except DoNotRetryError as exc:
        logger.fatal('Exception occured: {0}. Aborting.'.format(exc), extra=task_log(self))
        raise exc
    except BackoffRetryError as exc:
        if task_should_retry(self, exc):
            raise self.retry(exc=exc, countdown=5 ** self.request.retries)
        else:
            raise exc
    except RetryError as exc:
        if task_should_retry(self, exc):
            raise self.retry(exc=exc)
        else:
            raise exc
    collect_queue_length_metric()
    logger.info('Ingest suceeded', extra=task_log(self))


def task_should_retry(task, exception):
    if task.request.retries < task.max_retries:
        logger.warn(
            'Exception occured: {0}. Will retry'.format(exception),
            extra=task_log(task)
        )
        return True
    else:
        logger.fatal(
            'Exception occured: {0}. max_retries exceeded. Aborting.'.format(exception),
            extra=task_log(task)
        )
        return False


def collect_queue_length_metric():
    i = app.control.inspect()
    if i.reserved() or i.active():
        host_string = 'celery@{}'.format(platform.node())
        reserved = len(i.reserved()[host_string])
        active = len(i.active()[host_string]) - 1  # exclude this (done) task
        send_tsdb_metric('ingester.queue_length', reserved + active, async=False)
