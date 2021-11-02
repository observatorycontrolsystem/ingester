from unittest.mock import MagicMock, patch
import unittest
import os
import io
import tarfile
import hashlib
from copy import copy

import opentsdb_python_metrics.metric_wrappers
import dateutil

from ocs_archive.input.file import File
from ocs_archive.input.filefactory import FileFactory

from ocs_ingester.ingester import (Ingester, upload_file_and_ingest_to_archive, ingest_archive_record,
                                   upload_file_to_file_store, validate_fits_and_create_archive_record)
from ocs_ingester.exceptions import DoNotRetryError, NonFatalDoNotRetryError
from ocs_ingester.settings import settings

opentsdb_python_metrics.metric_wrappers.test_mode = True


FITS_PATH = os.path.join(
    os.path.dirname(__file__),
    'test_files/fits/'
)

OTHER_PATH = os.path.join(
    os.path.dirname(__file__),
    'test_files/other/'
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
    'lscnrs01-fl09-20171109-0049-e91.tar.gz'
)

PDF_FILE = os.path.join(
    OTHER_PATH,
    'cptnrs03-fa13-20150219-0001-e92-summary.pdf'
)


def mock_hashlib_md5(*args, **kwargs):
    class MockHash(object):
        def __init__(self):
            pass

        def hexdigest(self):
            return 'fakemd5'

    return MockHash()

# These mocks are in global namespace so they can apply to the mocked Ingester class below
archive_mock = MagicMock()
archive_mock.version_exists.return_value = False
filestore_mock = MagicMock()
filestore_mock.store_file = MagicMock(return_value={'md5': 'fakemd5'})

def mocked_ingester(datafile_real, fake_filestore, fake_archive):
    class MockIngester(Ingester):
        def __init__(self):
            super().__init__(datafile_real, filestore_mock, archive_mock)

    return MockIngester()


class TestIngesterMethods(unittest.TestCase):
    def test_create_archive_record(self):
        with open(FITS_FILE, 'rb') as fileobj:
            archive_record = validate_fits_and_create_archive_record(fileobj)
            self.assertEqual(archive_record['area']['type'], 'Polygon')
            self.assertIn('headers', archive_record)

    def test_upload_file_to_file_store(self):
        with open(FITS_FILE, 'rb') as fileobj:
            version = upload_file_to_file_store(fileobj)
            self.assertIn('md5', version)

    @patch('requests.post')
    def test_ingest_archive_record(self, post_mock):
        with open(FITS_FILE, 'rb') as fileobj:
            archive_record = validate_fits_and_create_archive_record(fileobj)
            version = upload_file_to_file_store(fileobj)
            ingest_archive_record(version, archive_record, api_root='http://fake')
            self.assertTrue(post_mock.called)


class TestIngester(unittest.TestCase):
    def setUp(self):
        # Since these are globally used mocks, we should reset them at the start of each test in here
        filestore_mock.reset_mock()
        archive_mock.reset_mock()
        hashlib.md5 = MagicMock(side_effect=mock_hashlib_md5)
        self.open_files = [File(open(os.path.join(FITS_PATH, f), 'rb')) for f in os.listdir(FITS_PATH)]
        self.data_files = [FileFactory.get_datafile_class_for_extension(open_file.extension)(open_file) for open_file in self.open_files]
        self.mock_metadata = {'PROPID': 'INGEST-TEST-2021',
                              'DATE-OBS': '2015-02-19T13:56:05.261',
                              'INSTRUME': 'nres03',
                              'SITEID': 'cpt',
                              'TELID': '1m0a',
                              'OBSTYPE': 'EXPOSE',
                              'BLKUID': 1234,
                              'RLEVEL': 92}
        self.ingesters = [
            Ingester(
                datafile=data_file,
                filestore=filestore_mock,
                archive=archive_mock
            )
            for data_file in self.data_files
        ]

    def tearDown(self):
        for file in self.open_files:
            file.fileobj.close()

    def test_ingest_file(self):
        for ingester in self.ingesters:
            ingester.ingest()
            self.assertTrue(filestore_mock.store_file.called)
            self.assertTrue(archive_mock.post_frame.called)

    @patch('ocs_ingester.ingester.Ingester', side_effect=mocked_ingester)
    def test_ingest_bytesio_file(self, ingester_mock):
        with io.BytesIO() as buf:
            with open(FITS_FILE, 'rb') as fileobj:
                buf.write(fileobj.read())
                upload_file_and_ingest_to_archive(buf, path='fake_path.fits')
                self.assertTrue(filestore_mock.store_file.called)
                self.assertTrue(archive_mock.post_frame.called)

    @patch('ocs_ingester.ingester.Ingester', side_effect=mocked_ingester)
    def test_ingest_bytesio_file_with_no_path(self, ingester_mock):
        # BytesIO objects have no name attr, and must specify a path
        with io.BytesIO() as buf:
            with open(FITS_FILE, 'rb') as fileobj:
                buf.write(fileobj.read())
                with self.assertRaises(DoNotRetryError):
                    upload_file_and_ingest_to_archive(buf)
        self.assertFalse(filestore_mock.store_file.called)
        self.assertFalse(archive_mock.post_frame.called)

    def test_ingest_file_already_exists(self):
        archive_mock.version_exists.return_value = True
        with self.assertRaises(NonFatalDoNotRetryError):
            self.ingesters[0].ingest()
        self.assertFalse(filestore_mock.store_file.called)
        self.assertFalse(archive_mock.post_frame.called)
        archive_mock.version_exists.return_value = False

    @patch('ocs_ingester.ingester.Ingester', side_effect=mocked_ingester)
    def test_required(self, ingester_mock):
        required_headers = ['fooheader']
        with self.assertRaises(DoNotRetryError):
            upload_file_and_ingest_to_archive(self.open_files[0].fileobj, required_headers=required_headers)
        self.assertFalse(ingester_mock.filestore.store_file.called)
        self.assertFalse(ingester_mock.archive.post_frame.called)

    @patch('ocs_ingester.ingester.Ingester', side_effect=mocked_ingester)
    def test_get_area(self, ingester_mock):
        with open(FITS_FILE, 'rb') as fileobj:
            upload_file_and_ingest_to_archive(fileobj)
            self.assertEqual('Polygon', archive_mock.post_frame.call_args[0][0]['area']['type'])
        with open(CAT_FILE, 'rb') as fileobj:
            upload_file_and_ingest_to_archive(fileobj)
            self.assertIsNone(archive_mock.post_frame.call_args[0][0]['area'])

    @patch('ocs_ingester.ingester.Ingester', side_effect=mocked_ingester)
    def test_blacklist(self, ingester_mock):
        with open(FITS_FILE, 'rb') as fileobj:
            blacklist_headers = ['', 'COMMENT', 'HISTORY']
            upload_file_and_ingest_to_archive(fileobj, blacklist_headers=blacklist_headers)
            self.assertNotIn('COMMENT', archive_mock.post_frame.call_args[0][0]['headers'].keys())

    def test_reduction_level(self):
        for ingester in self.ingesters:
            ingester.ingest()
            self.assertIn('reduction_level', archive_mock.post_frame.call_args[0][0].keys())

    @patch('ocs_ingester.ingester.Ingester', side_effect=mocked_ingester)
    def test_related(self, ingester_mock):
        with open(FITS_FILE, 'rb') as fileobj:
            upload_file_and_ingest_to_archive(fileobj)
            self.assertEqual(
                'bias_kb05_20150219_bin2x2',
                archive_mock.post_frame.call_args[0][0]['headers']['L1IDBIAS']
            )
            self.assertEqual(
                'dark_kb05_20150219_bin2x2',
                archive_mock.post_frame.call_args[0][0]['headers']['L1IDDARK']
            )
            self.assertEqual(
                'flat_kb05_20150219_SKYFLAT_bin2x2_V',
                archive_mock.post_frame.call_args[0][0]['headers']['L1IDFLAT']
            )

    @patch('ocs_ingester.ingester.Ingester', side_effect=mocked_ingester)
    def test_catalog_related(self, ingester_mock):
        with open(CAT_FILE, 'rb') as fileobj:
            upload_file_and_ingest_to_archive(fileobj)
            self.assertEqual(
                'cpt1m010-kb70-20151219-0073-e10',
                archive_mock.post_frame.call_args[0][0]['headers']['L1IDCAT']
            )

    @patch('ocs_archive.settings.settings.FILETYPE_MAPPING_OVERRIDES', {'.tar.gz': 'ocs_archive.input.lcotarwithfitsfile.LcoTarWithFitsFile'})
    @patch('ocs_ingester.ingester.Ingester', side_effect=mocked_ingester)
    def test_spectograph(self, ingester_mock):
        with open(SPECTRO_FILE, 'rb') as fileobj:
            upload_file_and_ingest_to_archive(fileobj)
            self.assertEqual(90, archive_mock.post_frame.call_args[0][0]['reduction_level'])
            self.assertTrue(dateutil.parser.parse(archive_mock.post_frame.call_args[0][0]['public_date']))

    @patch('ocs_archive.settings.settings.FILETYPE_MAPPING_OVERRIDES', {'.tar.gz': 'ocs_archive.input.lcotarwithfitsfile.LcoTarWithFitsFile'})
    @patch('ocs_ingester.ingester.Ingester', side_effect=mocked_ingester)
    def test_nres_package(self, ingester_mock):
        with open(NRES_FILE, 'rb') as fileobj:
            upload_file_and_ingest_to_archive(fileobj)
            self.assertEqual('Polygon', archive_mock.post_frame.call_args[0][0]['area']['type'])
            self.assertEqual(91, archive_mock.post_frame.call_args[0][0]['reduction_level'])
            self.assertEqual('TARGET', archive_mock.post_frame.call_args[0][0]['configuration_type'])
            self.assertTrue(dateutil.parser.parse(archive_mock.post_frame.call_args[0][0]['public_date']))

    @patch('ocs_ingester.ingester.Ingester', side_effect=mocked_ingester)
    def test_spectrograph_missing_meta(self, ingester_mock):
        tarfile.TarFile.getmembers = MagicMock(return_value=[])
        with self.assertRaises(DoNotRetryError):
            with open(SPECTRO_FILE, 'rb') as fileobj:
                upload_file_and_ingest_to_archive(fileobj)

    @patch('ocs_ingester.ingester.Ingester', side_effect=mocked_ingester)
    def test_empty_string_for_na(self, ingester_mock):
        with open(os.path.join(FITS_PATH, 'coj1m011-fl08-20151216-0049-b00.fits'), 'rb') as fileobj:
            upload_file_and_ingest_to_archive(fileobj)
            self.assertFalse(archive_mock.post_frame.call_args[0][0]['target_name'])
            self.assertIsNotNone(archive_mock.post_frame.call_args[0][0]['observation_date'])

    def test_reqnum_null_or_int(self):
        for ingester in self.ingesters:
            ingester.ingest()
            reqnum = archive_mock.post_frame.call_args[0][0]['request_id']
            try:
                self.assertIsNone(reqnum)
            except AssertionError:
                self.assertGreater(int(reqnum), -1)

    @patch('ocs_ingester.ingester.Ingester', side_effect=mocked_ingester)
    def test_ingest_pdf_no_meta(self, ingester_mock):
        with open(PDF_FILE, 'rb') as fileobj:
            with self.assertRaises(DoNotRetryError):
                upload_file_and_ingest_to_archive(fileobj)
            self.assertFalse(filestore_mock.store_file.called)
            self.assertFalse(archive_mock.post_frame.called)

    @patch('ocs_ingester.ingester.Ingester', side_effect=mocked_ingester)
    def test_ingest_pdf_missing_keyword(self, ingester_mock):
        bad_metadata = copy(self.mock_metadata)
        del bad_metadata['BLKUID']
        with open(PDF_FILE, 'rb') as fileobj:
            with self.assertRaises(DoNotRetryError):
                upload_file_and_ingest_to_archive(fileobj, file_metadata=bad_metadata)
            self.assertFalse(filestore_mock.store_file.called)
            self.assertFalse(archive_mock.post_frame.called)

    @patch('ocs_ingester.ingester.Ingester', side_effect=mocked_ingester)
    def test_ingest_pdf_with_meta(self, ingester_mock):
        with open(PDF_FILE, 'rb') as fileobj:
            upload_file_and_ingest_to_archive(fileobj, file_metadata=self.mock_metadata)
            self.assertTrue(filestore_mock.store_file.called)
            self.assertTrue(archive_mock.post_frame.called)
