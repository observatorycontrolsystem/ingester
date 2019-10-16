from requests.auth import HTTPBasicAuth
from opentsdb_python_metrics.metric_wrappers import metric_timer, send_tsdb_metric
from celery import Celery
from celery.exceptions import SoftTimeLimitExceeded
from logging.config import dictConfig
import logging
import os
import requests

from settings.log_config import logConf
from ingester.archive import ArchiveService
from ingester.utils.fits import get_fits_from_path
from ingester.s3 import S3Service
from ingester.postproc import PostProcService
from ingester.ingester import Ingester
from ingester.exceptions import RetryError, DoNotRetryError, BackoffRetryError, NonFatalDoNotRetryError


dictConfig(logConf)
logger = logging.getLogger('ingester')
app = Celery('tasks')
app.config_from_object('settings.celery_config.celery_config')


def task_log(task):
    path = task.request.kwargs.get('path', '')
    return {
        'tags': {
            'filename': os.path.basename(path),
            'path': path,
            'attempt': task.request.retries + 1
        }
    }


@app.task(bind=True, max_retries=3, default_retry_delay=3 * 60)
@metric_timer('ingester', asynchronous=False)
def do_ingest(self, path, bucket, api_root, auth_token, broker_url, required_headers, blacklist_headers):
    """
    Create a new instance of an Ingester and run it's
    ingest() method on a specific path
    """
    logger.info('Starting ingest', extra=task_log(self))

    # Service instantiation
    archive = ArchiveService(api_root=api_root, auth_token=auth_token)
    s3 = S3Service(bucket)
    post_proc = PostProcService(broker_url)
    try:
        fileobj = get_fits_from_path(path)
        ingester = Ingester(fileobj, s3, archive, required_headers, blacklist_headers)
        ingested_frame = ingester.ingest()
        post_proc.post_to_archived_queue(ingested_frame)
    except DoNotRetryError as exc:
        logger.fatal('Exception occured: {0}. Aborting.'.format(exc), extra=task_log(self))
        raise exc
    except NonFatalDoNotRetryError as exc:
        logger.warning('Non-fatal Exception occured: {0}. Aborting.'.format(exc), extra=task_log(self))
        return
    except BackoffRetryError as exc:
        if task_should_retry(self, exc):
            raise self.retry(exc=exc, countdown=5 ** self.request.retries)
        else:
            raise exc
    except (IOError, RetryError, SoftTimeLimitExceeded) as exc:
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
        logger.warning(
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
    send_tsdb_metric('ingester.queue_length', response['messages'])


@app.task
@metric_timer('archive.response_time', asynchronous=False)
def total_holdings(api_root, auth_token):
    response = requests.get(
        '{0}frames/'.format(api_root),
        headers={'Authorization': 'Token {0}'.format(auth_token)}
    ).json()
    send_tsdb_metric('archive.total_products', response['count'])
