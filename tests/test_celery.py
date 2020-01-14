import unittest
from unittest.mock import patch, mock_open

from celery.exceptions import SoftTimeLimitExceeded

from lco_ingester.settings import celery_config
from lco_ingester.ingester import Ingester
from lco_ingester.archive import PostProcService
from tasks import do_ingest
from lco_ingester.exceptions import DoNotRetryError, BackoffRetryError


class TestCelery(unittest.TestCase):
    def setUp(self):
        celery_config.celery_config['task_always_eager'] = True
        postproc_patcher = patch.object(PostProcService, 'post_to_archived_queue')
        self.postproc_mock = postproc_patcher.start()
        self.addCleanup(postproc_patcher.stop)

    @patch.object(Ingester, 'ingest')
    @mock_open(read_data=b'1234567890')
    def test_task_success(self, open_mock, ingest_mock):
        ingest_mock.return_value = {}
        result = do_ingest.delay('', None, None, None, None, None, None)
        self.assertTrue(result)
        self.assertTrue(self.postproc_mock.called)

    @patch.object(Ingester, 'ingest', side_effect=DoNotRetryError('missing file'))
    @mock_open(read_data=b'1234567890')
    def test_task_failure(self, open_mock, ingest_mock):
        result = do_ingest.delay('', None, None, None, None, None, None)
        self.assertIs(result.result.__class__, DoNotRetryError)
        self.assertTrue(result.failed())

    @patch.object(Ingester, 'ingest', side_effect=BackoffRetryError('Timeout'))
    @mock_open(read_data=b'1234567890')
    def test_task_retry(self, open_mock, ingest_mock):
        result = do_ingest.delay('', None, None, None, None, None, None)
        self.assertEqual(ingest_mock.call_count, 4)
        self.assertIs(result.result.__class__, BackoffRetryError)
        self.assertTrue(result.failed())

    @patch.object(Ingester, 'ingest', side_effect=Exception('An unexpected exception'))
    @mock_open(read_data=b'1234567890')
    def test_task_unexpected_exception(self, open_mock, ingest_mock):
        result = do_ingest.delay('', None, None, None, None, None, None)
        self.assertEqual(ingest_mock.call_count, 4)
        self.assertIs(result.result.__class__, Exception)
        self.assertTrue(result.failed())

    @patch.object(Ingester, 'ingest', side_effect=SoftTimeLimitExceeded())
    @mock_open(read_data=b'1234567890')
    def test_task_softimelimit_exceeded(self, open_mock, ingest_mock):
        result = do_ingest.delay('', None, None, None, None, None, None)
        self.assertEqual(ingest_mock.call_count, 4)
        self.assertIs(result.result.__class__, SoftTimeLimitExceeded)
        self.assertTrue(result.failed())
