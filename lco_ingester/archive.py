import logging
from datetime import datetime

import requests
from opentsdb_python_metrics.metric_wrappers import metric_timer, SendMetricMixin

from lco_ingester.postproc import PostProcService
from lco_ingester.utils.fits import obs_end_time_from_dict
from lco_ingester.exceptions import BackoffRetryError, RetryError

logger = logging.getLogger('lco_ingester')


class ArchiveService(SendMetricMixin):
    def __init__(self, api_root, auth_token, broker_url):
        self.api_root = api_root
        self.headers = {'Authorization': 'Token {}'.format(auth_token)}
        self.broker_url = broker_url

    def handle_response(self, response):
        try:
            response.raise_for_status()
        except requests.exceptions.ConnectionError as exc:
            raise BackoffRetryError(exc)
        except requests.exceptions.HTTPError as exc:
            raise RetryError(exc)
        return response.json()

    def version_exists(self, md5):
        response = requests.get(
            '{0}versions/?md5={1}'.format(self.api_root, md5), headers=self.headers
        )
        result = self.handle_response(response)
        try:
            return result['count'] > 0
        except KeyError as e:
            raise BackoffRetryError(e)

    @metric_timer('ingester.post_frame')
    def post_frame(self, fits_dict):
        response = requests.post(
            '{0}frames/'.format(self.api_root), json=fits_dict, headers=self.headers
        )
        result = self.handle_response(response)
        logger.info('Ingester posted frame to archive', extra={
            'tags': {
                'filename': result.get('filename'),
                'request_num': fits_dict.get('REQNUM'),
                'PROPID': result.get('PROPID'),
                'id': result.get('id')
            }
        })
        # Add some useful information from the result
        fits_dict['frameid'] = result.get('id')
        fits_dict['filename'] = result.get('filename')
        fits_dict['url'] = result.get('url')
        # Send frame that was posted to the fits exchange
        post_proc = PostProcService(self.broker_url)
        post_proc.post_to_archived_queue(fits_dict)
        # Record metric for the ingest lag (time between date of image vs date ingested)
        ingest_lag = datetime.utcnow() - obs_end_time_from_dict(fits_dict)
        self.send_metric('ingester.ingest_lag', ingest_lag.total_seconds())
        return fits_dict
