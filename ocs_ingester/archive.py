import logging
from datetime import datetime, timedelta
from dateutil.parser import parse

import requests
from opentsdb_python_metrics.metric_wrappers import SendMetricMixin

from ocs_ingester.utils import metrics
from ocs_ingester.exceptions import BackoffRetryError, DoNotRetryError
from ocs_ingester.settings import settings as ingester_settings

from ocs_archive.settings import settings as archive_settings

logger = logging.getLogger('ocs_ingester')


def obs_end_time_from_dict(archive_record):
    obs_date = parse(archive_record.get('observation_date'))
    end_time = archive_record.get('headers', {}).get(archive_settings.OBSERVATION_END_TIME_KEY)
    if end_time:
        end_time = parse(end_time)
        # observation end is just a time - we need the date as well to be sure when this is
        end_date = obs_date.date()
        if abs(obs_date.hour - end_time.hour) > 12:
            # There was a date rollover during this observation, so set the date for utstop
            end_date += timedelta(days=1)

        return datetime.combine(end_date, end_time.time())
    elif archive_record.get('exposure_time'):
        return obs_date + timedelta(seconds=archive_record['exposure_time'])
    return obs_date


class ArchiveService(SendMetricMixin):
    def __init__(self, api_root, auth_token):
        self.api_root = api_root
        self.headers = {'Authorization': 'Token {}'.format(auth_token)}

    def handle_response(self, response):
        try:
            response.raise_for_status()
        except requests.exceptions.ConnectionError as exc:
            raise BackoffRetryError(exc)
        except requests.exceptions.HTTPError as exc:
            # HTTP 4xx errors are "client errors", meaning that the client sent
            # an incorrectly formatted request. There is no reason to retry an
            # incorrectly formatted request; it will only fail again.
            if 400 <= response.status_code < 500:
                raise DoNotRetryError(exc)

            # All other responses should back off and retry at a later time.
            # The most likely cause is that the HTTP server is unavailable
            # or overloaded, so we should try again later.
            raise BackoffRetryError(exc)

        # Return JSON data to client
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

    @metrics.method_timer('ingester.post_frame')
    def post_frame(self, archive_record):
        response = requests.post(
            '{0}frames/'.format(self.api_root), json=archive_record, headers=self.headers
        )
        result = self.handle_response(response)
        logger.info('Ingester posted frame to archive', extra={
            'tags': {
                'filename': result.get('filename'),
                'request_id': archive_record.get('request_id'),
                'proposal_id': result.get('proposal_id'),
                'id': result.get('id')
            }
        })
        # Add some useful information from the result
        archive_record['frameid'] = result.get('id')
        archive_record['filename'] = result.get('filename')
        archive_record['url'] = result.get('url')
        # Record metric for the ingest lag (time between date of image vs date ingested)
        ingest_lag = datetime.utcnow() - obs_end_time_from_dict(archive_record)
        self.send_metric(
            metric_name='ingester.ingest_lag',
            value=ingest_lag.total_seconds(),
            asynchronous=ingester_settings.SUBMIT_METRICS_ASYNCHRONOUSLY,
            **ingester_settings.EXTRA_METRICS_TAGS
        )
        return archive_record
