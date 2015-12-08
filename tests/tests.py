import unittest
import os
import settings
from unittest.mock import patch
from tasks import do_ingest
from ingester.ingester import Ingester
from ingester.exceptions import DoNotRetryError, BackoffRetryError
import opentsdb_python_metrics.metric_wrappers
opentsdb_python_metrics.metric_wrappers.test_mode = True


FITS_PATH = os.path.join(
    os.path.dirname(__file__),
    'fits/coj1m011-kb05-20150219-0125-e90.fits'
)

test_bucket = 'testbucket'


class TestCelery(unittest.TestCase):
    def setUp(self):
        settings.CELERY_ALWAYS_EAGER = True

    @patch.object(Ingester, 'ingest', return_value=None)
    def test_task_success(self, ingest_mock):
        result = do_ingest.delay(None, None)
        self.assertTrue(result.successful())

    @patch.object(Ingester, 'ingest', side_effect=DoNotRetryError('missing file'))
    def test_task_failure(self, ingest_mock):
        result = do_ingest.delay(None, None)
        self.assertIs(result.result.__class__, DoNotRetryError)
        self.assertTrue(result.failed())

    @patch.object(Ingester, 'ingest',  side_effect=BackoffRetryError('Timeout'))
    def test_task_retry(self, ingest_mock):
        result = do_ingest.delay(None, None)
        self.assertEqual(ingest_mock.call_count, 4)
        self.assertIs(result.result.__class__, BackoffRetryError)
        self.assertTrue(result.failed())


@patch('boto3.resource')
class TestIngester(unittest.TestCase):
    def test_ingest_file(self, s3_mock):
        ingester = Ingester(FITS_PATH, 'testbucket')
        ingester.ingest()
        self.assertTrue(s3_mock.called)

    def test_missing_file(self, s3_mock):
        ingester = Ingester('/path/does/not/exist.fits', 'testbucket')
        with self.assertRaises(DoNotRetryError):
            ingester.ingest()
        self.assertFalse(s3_mock.called)

    def test_required(self, s3_mock):
        ingester = Ingester(FITS_PATH, 'test_bucket', required_headers=['fooheader'])
        with self.assertRaises(DoNotRetryError):
            ingester.ingest()
        self.assertFalse(s3_mock.called)
        ingester = Ingester(FITS_PATH, 'test_bucket', required_headers=['DAY-OBS'])
        ingester.ingest()
        self.assertTrue(s3_mock.called)
