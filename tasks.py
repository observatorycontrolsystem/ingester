from celery import Celery
from ingester.ingester import Ingester
from ingester.exceptions import RetryError, DoNotRetryError, BackoffRetryError, NonFatalDoNotRetryError
import logging
import os
import requests
from requests.auth import HTTPBasicAuth
from opentsdb_python_metrics.metric_wrappers import metric_timer, send_tsdb_metric

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
    except NonFatalDoNotRetryError as exc:
        logger.warn('Non-fatal Exception occured: {0}. Aborting.'.format(exc), extra=task_log(self))
        return
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
    except Exception as exc:
        logger.fatal('Unexpected exception: {0} Will retry.'.format(exc), extra=task_log(self))
        raise self.retry(exc=exc)

    logger.info('Ingest succeeded', extra=task_log(self))


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


@app.task
def collect_queue_length_metric(rabbit_api_root):
    response = requests.get(
        '{0}api/queues/%2f/celery/'.format(rabbit_api_root),
        auth=HTTPBasicAuth('guest', 'guest')
    ).json()
    send_tsdb_metric('ingester.queue_length', response['messages'], async=False)


@app.task
@metric_timer('archive.response_time', async=False)
def total_holdings(api_root, auth_token):
    response = requests.get(
        '{0}frames/'.format(api_root),
        headers={'Authorization': 'Token {0}'.format(auth_token)}
    ).json()
    send_tsdb_metric('archive.total_products', response['count'], async=False)
