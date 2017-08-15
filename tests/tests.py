from unittest.mock import MagicMock
import opentsdb_python_metrics.metric_wrappers
import unittest
import os
import tarfile
import dateutil
import hashlib

from ingester.ingester import Ingester
from ingester.exceptions import DoNotRetryError, RetryError
import settings

opentsdb_python_metrics.metric_wrappers.test_mode = True


FITS_PATH = os.path.join(
    os.path.dirname(__file__),
    'fits/'
)
FITS_FILE = os.path.join(
    FITS_PATH,
    'coj1m011-kb05-20150219-0125-e90.fits.fz'
)
CAT_FILE = os.path.join(
    FITS_PATH,
    'cpt1m010-kb70-20151219-0073-e10_cat.fits.fz'
)
SPECTRO_FILE = os.path.join(
    FITS_PATH,
    'KEY2014A-002_0000483537_ftn_20160119_57407.tar.gz'
)

NRES_FILE = os.path.join(
    FITS_PATH,
    'lscnrs01-fl09-20170716-0016-e91.tar.gz'
)


def mock_hashlib_md5(*args, **kwargs):
    class MockHash(object):
        def __init__(self):
            pass

        def hexdigest(self):
            return 'fakemd5'

    return MockHash()


class TestIngester(unittest.TestCase):
    def setUp(self):
        hashlib.md5 = MagicMock(side_effect=mock_hashlib_md5)
        fits_files = [os.path.join(FITS_PATH, f) for f in os.listdir(FITS_PATH)]
        self.archive_mock = MagicMock()
        self.s3_mock = MagicMock()
        self.s3_mock.upload_file = MagicMock(return_value={'md5': 'fakemd5'})
        self.post_proc_mock = MagicMock()
        self.ingesters = [
            Ingester(
                path=path,
                s3=self.s3_mock,
                archive=self.archive_mock,
                post_proc=self.post_proc_mock,
                required_headers=settings.REQUIRED_HEADERS,
                blacklist_headers=settings.HEADER_BLACKLIST,
                )
            for path in fits_files
        ]

    def create_ingester_for_path(self, path=FITS_FILE):
        ingester = Ingester(
            path=path,
            s3=self.s3_mock,
            archive=self.archive_mock,
            post_proc=self.post_proc_mock,
            blacklist_headers=settings.HEADER_BLACKLIST,
            required_headers=settings.REQUIRED_HEADERS
        )
        return ingester

    def test_ingest_file(self):
        for ingester in self.ingesters:
            ingester.ingest()
            self.assertTrue(self.s3_mock.upload_file.called)
            self.assertTrue(self.archive_mock.post_frame.called)
            self.assertTrue(self.post_proc_mock.post_to_archived_queue.called)

    def test_missing_file(self):
        ingester = self.create_ingester_for_path('/path/doesnot/exist.fits.fz')
        with self.assertRaises(RetryError):
            ingester.ingest()
        self.assertFalse(self.s3_mock.upload_file.called)
        self.assertFalse(self.archive_mock.post_frame.called)

    def test_required(self):
        ingester = self.ingesters[0]
        ingester.required_headers = ['fooheader']
        with self.assertRaises(DoNotRetryError):
            ingester.ingest()
        self.assertFalse(self.s3_mock.upload_file.called)
        self.assertFalse(self.archive_mock.post_frame.called)

    def test_get_area(self):
        ingester = self.create_ingester_for_path(FITS_FILE)
        ingester.ingest()
        self.assertEqual('Polygon', self.archive_mock.post_frame.call_args[0][0]['area']['type'])
        ingester = self.create_ingester_for_path(CAT_FILE)
        ingester.ingest()
        self.assertIsNone(self.archive_mock.post_frame.call_args[0][0]['area'])

    def test_blacklist(self):
        ingester = self.ingesters[0]
        ingester.blacklist_headers = ['DAY-OBS', '', 'COMMENT', 'HISTORY']
        ingester.ingest()
        self.assertNotIn('DAY-OBS', self.archive_mock.post_frame.call_args[0][0].keys())

    def test_reduction_level(self):
        for ingester in self.ingesters:
            ingester.ingest()
            self.assertIn('RLEVEL', self.archive_mock.post_frame.call_args[0][0].keys())

    def test_related(self):
        ingester = self.create_ingester_for_path(FITS_FILE)
        ingester.ingest()
        self.assertEqual(
            'bias_kb05_20150219_bin2x2',
            self.archive_mock.post_frame.call_args[0][0]['L1IDBIAS']
        )
        self.assertEqual(
            'dark_kb05_20150219_bin2x2',
            self.archive_mock.post_frame.call_args[0][0]['L1IDDARK']
        )
        self.assertEqual(
            'flat_kb05_20150219_SKYFLAT_bin2x2_V',
            self.archive_mock.post_frame.call_args[0][0]['L1IDFLAT']
        )

    def test_catalog_related(self):
        ingester = self.create_ingester_for_path(CAT_FILE)
        ingester.ingest()
        self.assertEqual(
            'cpt1m010-kb70-20151219-0073-e10',
            self.archive_mock.post_frame.call_args[0][0]['L1IDCAT']
        )

    def test_spectograph(self):
        ingester = self.create_ingester_for_path(SPECTRO_FILE)
        ingester.ingest()
        self.assertEqual(90, self.archive_mock.post_frame.call_args[0][0]['RLEVEL'])
        self.assertTrue(dateutil.parser.parse(self.archive_mock.post_frame.call_args[0][0]['L1PUBDAT']))

    def test_nres_package(self):
        ingester = self.create_ingester_for_path(NRES_FILE)
        ingester.ingest()
        self.assertEqual(91, self.archive_mock.post_frame.call_args[0][0]['RLEVEL'])
        self.assertEqual('TARGET', self.archive_mock.post_frame.call_args[0][0]['OBSTYPE'])
        self.assertTrue(dateutil.parser.parse(self.archive_mock.post_frame.call_args[0][0]['L1PUBDAT']))

    def test_spectrograph_missing_meta(self):
        tarfile.TarFile.getmembers = MagicMock(return_value=[])
        ingester = self.create_ingester_for_path(SPECTRO_FILE)
        with self.assertRaises(DoNotRetryError):
            ingester.ingest()

    def test_empty_string_for_na(self):
        ingester = self.create_ingester_for_path(
            os.path.join(FITS_PATH, 'coj1m011-fl08-20151216-0049-b00.fits')
        )
        ingester.ingest()
        self.assertFalse(self.archive_mock.post_frame.call_args[0][0]['OBJECT'])
        self.assertTrue(self.archive_mock.post_frame.call_args[0][0]['DATE-OBS'])

    def test_reqnum_null_or_int(self):
        for ingester in self.ingesters:
            ingester.ingest()
            reqnum = self.archive_mock.post_frame.call_args[0][0]['REQNUM']
            try:
                self.assertIsNone(reqnum)
            except AssertionError:
                self.assertGreater(int(reqnum), -1)
