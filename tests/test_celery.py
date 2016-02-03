import unittest
import settings
from unittest.mock import patch
from ingester.ingester import Ingester
from tasks import do_ingest
from ingester.exceptions import DoNotRetryError, BackoffRetryError


class TestCelery(unittest.TestCase):
    def setUp(self):
        settings.CELERY_ALWAYS_EAGER = True

    @patch.object(Ingester, 'ingest', return_value=None)
    def test_task_success(self, ingest_mock):
        result = do_ingest.delay(None, None, None, None, None, None)
        self.assertTrue(result)

    @patch.object(Ingester, 'ingest', side_effect=DoNotRetryError('missing file'))
    def test_task_failure(self, ingest_mock):
        result = do_ingest.delay(None, None, None, None, None, None)
        self.assertIs(result.result.__class__, DoNotRetryError)
        self.assertTrue(result.failed())

    @patch.object(Ingester, 'ingest',  side_effect=BackoffRetryError('Timeout'))
    def test_task_retry(self, ingest_mock):
        result = do_ingest.delay(None, None, None, None, None, None)
        self.assertEqual(ingest_mock.call_count, 4)
        self.assertIs(result.result.__class__, BackoffRetryError)
        self.assertTrue(result.failed())
