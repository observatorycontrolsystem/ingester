from unittest.mock import patch
import unittest
import os
from datetime import datetime
from requests.exceptions import ConnectionError, HTTPError

from ocs_ingester.archive import ArchiveService, obs_end_time_from_dict
from ocs_ingester.ingester import frame_exists
from ocs_ingester.exceptions import BackoffRetryError, DoNotRetryError


FITS_PATH = os.path.join(
    os.path.dirname(__file__),
    'test_files/fits/'
)

FITS_FILE = os.path.join(
    FITS_PATH,
    'coj1m011-kb05-20150219-0125-e90.fits.fz'
)


def mocked_requests_get(*args, **kwargs):
    class MockResponse(object):
        def __init__(self, json_data, exception, status_code):
            self.json_data = json_data
            self.exception = exception
            self.status_code = status_code

        def json(self):
            return self.json_data

        def raise_for_status(self):
            if self.exception:
                raise self.exception
            else:
                return None

    if args[0].startswith('http://return1/'):
        return MockResponse({'count': 1}, None, 400)

    if args[0].startswith('http://return404/'):
        return MockResponse(None, HTTPError, 404)

    if args[0].startswith('http://badconnection/'):
        return MockResponse(None, ConnectionError, 500)

    return MockResponse({'count': 0}, None, 200)


@patch('requests.get', side_effect=mocked_requests_get)
@patch('requests.post')
class TestArchiveService(unittest.TestCase):
    def test_archive_post(self, post_mock, get_mock):
        archive_service = ArchiveService(api_root='http://fake/', auth_token='')
        archive_service.post_frame({'observation_date': datetime.utcnow().isoformat()})
        self.assertTrue(post_mock.called)
        self.assertEqual(post_mock.call_args[0][0], 'http://fake/frames/')

    def test_existing_md5(self, post_mock, get_mock):
        archive_service = ArchiveService(api_root='http://return1/', auth_token='')
        self.assertTrue(archive_service.version_exists(''))
        self.assertFalse(post_mock.called)

    def test_frame_exists(self, post_mock, get_mock):
        with open(FITS_FILE, 'rb') as fileobj:
            exists = frame_exists(fileobj, api_root='http://return1/')
            self.assertTrue(exists)

    def test_non_existing_md5(self, post_mock, get_mock):
        archive_service = ArchiveService(api_root='http://fake/', auth_token='')
        self.assertFalse(archive_service.version_exists(''))
        self.assertTrue(get_mock.called)

    def test_bad_response(self, post_mock, get_mock):
        archive_service = ArchiveService(api_root='http://return404/', auth_token='')
        with self.assertRaises(DoNotRetryError):
            archive_service.version_exists('')
        self.assertFalse(post_mock.called)

    def test_bad_connection(self, post_mock, get_mock):
        archive_service = ArchiveService(api_root='http://badconnection/', auth_token='')
        with self.assertRaises(BackoffRetryError):
            archive_service.version_exists('')
        self.assertFalse(post_mock.called)

    def test_get_obs_end_date_obs_date_only(self, post_mock, get_mock):
        archive_headers = {'observation_date': '2021-10-10T20:00:00'}
        end_date = obs_end_time_from_dict(archive_headers)
        self.assertEqual(end_date.isoformat(), archive_headers['observation_date'])

    def test_get_obs_end_date_with_exposure_time(self, post_mock, get_mock):
        archive_headers = {'observation_date': '2021-10-10T20:00:00', 'exposure_time': 375}
        end_date = obs_end_time_from_dict(archive_headers)
        self.assertEqual(end_date.isoformat(), '2021-10-10T20:06:15')

    @patch('ocs_archive.settings.settings.OBSERVATION_END_TIME_KEY', 'UTSTOP')
    def test_get_obs_end_date_with_end_time(self, post_mock, get_mock):
        archive_headers = {'observation_date': '2021-10-10T20:00:00', 'headers': {'UTSTOP': '23:00:00'}}
        end_date = obs_end_time_from_dict(archive_headers)
        self.assertEqual(end_date.isoformat(), '2021-10-10T23:00:00')

    @patch('ocs_archive.settings.settings.OBSERVATION_END_TIME_KEY', 'UTSTOP')
    def test_get_obs_end_date_with_end_time_next_day(self, post_mock, get_mock):
        archive_headers = {'observation_date': '2021-10-10T20:00:00', 'headers': {'UTSTOP': '04:00:00'}}
        end_date = obs_end_time_from_dict(archive_headers)
        self.assertEqual(end_date.isoformat(), '2021-10-11T04:00:00')
