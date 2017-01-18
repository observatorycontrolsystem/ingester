import unittest
import settings
from unittest.mock import patch
from celery.exceptions import SoftTimeLimitExceeded

from ingester.ingester import Ingester
from tasks import do_ingest
from ingester.exceptions import DoNotRetryError, BackoffRetryError


class TestCelery(unittest.TestCase):
    def setUp(self):
        settings.task_always_eager = True

    @patch.object(Ingester, 'ingest', return_value=None)
    def test_task_success(self, ingest_mock):
        result = do_ingest.delay(None, None, None, None, None, None, None)
        self.assertTrue(result)

    @patch.object(Ingester, 'ingest', side_effect=DoNotRetryError('missing file'))
    def test_task_failure(self, ingest_mock):
        result = do_ingest.delay(None, None, None, None, None, None, None)
        self.assertIs(result.result.__class__, DoNotRetryError)
        self.assertTrue(result.failed())

    @patch.object(Ingester, 'ingest',  side_effect=BackoffRetryError('Timeout'))
    def test_task_retry(self, ingest_mock):
        result = do_ingest.delay(None, None, None, None, None, None, None)
        self.assertEqual(ingest_mock.call_count, 4)
        self.assertIs(result.result.__class__, BackoffRetryError)
        self.assertTrue(result.failed())

    @patch.object(Ingester, 'ingest',  side_effect=Exception('An unexpected exception'))
    def test_task_unexpected_exception(self, ingest_mock):
        result = do_ingest.delay(None, None, None, None, None, None, None)
        self.assertEqual(ingest_mock.call_count, 4)
        self.assertIs(result.result.__class__, Exception)
        self.assertTrue(result.failed())

    @patch.object(Ingester, 'ingest',  side_effect=SoftTimeLimitExceeded())
    def test_task_softimelimit_exceeded(self, ingest_mock):
        result = do_ingest.delay(None, None, None, None, None, None, None)
        self.assertEqual(ingest_mock.call_count, 4)
        self.assertIs(result.result.__class__, SoftTimeLimitExceeded)
        self.assertTrue(result.failed())
