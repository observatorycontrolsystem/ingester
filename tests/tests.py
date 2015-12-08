import unittest
import os
import settings
from unittest.mock import patch
from tasks import do_ingest
from ingester.utils.fits import fits_to_dict
from ingester.ingester import Ingester
import opentsdb_python_metrics.metric_wrappers
opentsdb_python_metrics.metric_wrappers.test_mode = True


FITS_PATH = os.path.join(
    os.path.dirname(__file__),
    'fits/coj1m011-kb05-20150219-0125-e90.fits'
)

test_bucket = 'testbucket'


@patch('boto3.resource')
class TestCelery(unittest.TestCase):
    def setUp(self):
        settings.CELERY_ALWAYS_EAGER = True

    def test_task_success(self, s3_mock):
        result = do_ingest.delay(FITS_PATH, test_bucket)
        self.assertTrue(result.successful())

    def test_task_failure(self, s3_mock):
        result = do_ingest.delay('/pathdoesnot/exit.fits', test_bucket)
        self.assertIs(result.result.__class__, FileNotFoundError)
        self.assertTrue(result.failed())

    @patch.object(Ingester, 'upload_to_s3',  side_effect=ConnectionError())
    def test_task_retry(self, upload_mock, s3_mock):
        result = do_ingest.delay(FITS_PATH, test_bucket)
        self.assertEqual(upload_mock.call_count, 4)
        self.assertTrue(result.failed())


class TestUtils(unittest.TestCase):
    def test_fits_to_dict(self):
        result = fits_to_dict(FITS_PATH, settings.HEADER_BLACKLIST)
        for header in settings.HEADER_BLACKLIST:
            self.assertNotIn(header, result.keys())
        self.assertEqual(
            'coj1m011-kb05-20150219-0125-e00.fits',
            result['ORIGNAME']
        )
