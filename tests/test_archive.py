from unittest.mock import patch
import unittest
from datetime import datetime
from requests.exceptions import ConnectionError, HTTPError

from lco_ingester.archive import ArchiveService
from lco_ingester.exceptions import BackoffRetryError, DoNotRetryError


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
        archive_service.post_frame({'DATE-OBS': datetime.utcnow().isoformat()})
        self.assertTrue(post_mock.called)
        self.assertEqual(post_mock.call_args[0][0], 'http://fake/frames/')

    def test_existing_md5(self, post_mock, get_mock):
        archive_service = ArchiveService(api_root='http://return1/', auth_token='')
        self.assertTrue(archive_service.version_exists(''))
        self.assertFalse(post_mock.called)

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
