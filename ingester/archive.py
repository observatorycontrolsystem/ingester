import requests
import logging

from ingester.exceptions import BackoffRetryError, NonFatalDoNotRetryError, RetryError

logger = logging.getLogger('ingester')


class ArchiveService(object):
    def __init__(self, *args, **kwargs):
        self.api_root = kwargs.get('api_root')
        self.headers = {'Authorization': 'Token {}'.format(kwargs.get('auth_token'))}

    def handle_response(self, response):
        try:
            response.raise_for_status()
        except requests.exceptions.ConnectionError as exc:
            raise BackoffRetryError(exc)
        except requests.exceptions.HTTPError as exc:
            raise RetryError(exc)
        return response.json()

    def check_for_existing_version(self, md5):
        response = requests.get(
            '{0}versions/?md5={1}'.format(self.api_root, md5), headers=self.headers
        )
        result = self.handle_response(response)
        try:
            if result['count'] > 0:
                raise NonFatalDoNotRetryError('Version with this md5 already exists')
        except KeyError as exc:
            raise BackoffRetryError(exc)

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
        return result.get('id')
