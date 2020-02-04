import unittest
from unittest.mock import MagicMock
import os

from lco_ingester.fits import FitsDict
from lco_ingester.utils.fits import File


class TestFits(unittest.TestCase):
    def setUp(self):
        self.fileobj = MagicMock(spec=True)
        self.fileobj.name = ''

    def test_null_values(self):
        fd = FitsDict(File(self.fileobj), [], [])
        fd.fits_dict = {
            'OBJECT': 'UNKNOWN',
            'PROPID': 'N/A',
            'BLKUID': 'N/A',
            'INSTRUME': 'fl03',
            'REQNUM': 'UNSPECIFIED'
        }
        fd.normalize_null_values()
        self.assertEqual('', fd.fits_dict['OBJECT'])
        self.assertEqual('', fd.fits_dict['PROPID'])
        self.assertIsNone(fd.fits_dict['BLKUID'])
        self.assertEqual('fl03', fd.fits_dict['INSTRUME'])
        self.assertIsNone(fd.fits_dict['REQNUM'])

    def test_remove_blacklist(self):
        fd = FitsDict(File(self.fileobj), [], ['FOO'])
        fd.fits_dict = {
            'FOO': 'BAR',
            'BAZ': 'BAM'
        }
        fd.remove_blacklist_headers()
        self.assertNotIn('FOO', fd.fits_dict.keys())
        self.assertIn('BAZ', fd.fits_dict.keys())

    def test_rlevel_present(self):
        fd = FitsDict(File(self.fileobj), [], [])
        fd.fits_dict = {'RLEVEL': 91}
        fd.check_rlevel()
        self.assertEqual(91, fd.fits_dict['RLEVEL'])

    def test_rlevel_missing(self):
        self.fileobj.name = 'something-e11.fits.fz'
        fd = FitsDict(File(self.fileobj), [], [])
        fd.fits_dict = {}
        fd.check_rlevel()
        self.assertEqual(11, fd.fits_dict['RLEVEL'])

    def test_dayobs_missing(self):
        fd = FitsDict(File(self.fileobj), [], [])
        fd.fits_dict = {'DATE-OBS': '2020-01-31T20:09:56.956'}
        fd.check_dayobs()
        self.assertEqual('20200131', fd.fits_dict['DAY-OBS'])

    def test_catalog_file(self):
        self.fileobj.name = 'something-e90_cat.fits.fz'
        fd = FitsDict(File(self.fileobj), [], [])
        fd.fits_dict = {}
        fd.check_catalog()
        self.assertEqual('something-e90', fd.fits_dict['L1IDCAT'])

    def test_public_date_public_file(self):
        fd = FitsDict(File(self.fileobj), [], [])
        fd.fits_dict = {
            'PROPID': 'EPOTHING',
            'DATE-OBS': '2016-04-01T00:00:00+00:00',
            'OBSTYPE': 'EXPOSE'
        }
        fd.set_public_date()
        self.assertEqual(fd.fits_dict['L1PUBDAT'], fd.fits_dict['DATE-OBS'])

    def test_public_date_private_file(self):
        fd = FitsDict(File(self.fileobj), [], [])
        fd.fits_dict = {
            'PROPID': 'LCO2015',
            'DATE-OBS': '2016-04-01T00:00:00+00:00',
            'OBSTYPE': 'EXPOSE'
        }
        fd.set_public_date()
        self.assertEqual(fd.fits_dict['L1PUBDAT'], '2017-04-01T00:00:00+00:00')

    def test_public_date_private_t00(self):
        self.fileobj.name = 'whatever-t00'
        fd = FitsDict(File(self.fileobj), [], [])
        fd.fits_dict = {
            'PROPID': 'LCO2015',
            'DATE-OBS': '2016-04-01T00:00:00+00:00',
            'OBSTYPE': 'EXPOSE'
        }
        fd.set_public_date()
        self.assertEqual(fd.fits_dict['L1PUBDAT'], '3014-08-03T00:00:00+00:00')

    def test_public_date_private_x00(self):
        self.fileobj.name = 'whatever-x00'
        fd = FitsDict(File(self.fileobj), [], [])
        fd.fits_dict = {
            'PROPID': 'LCO2015',
            'DATE-OBS': '2016-04-01T00:00:00+00:00',
            'OBSTYPE': 'EXPOSE'
        }
        fd.set_public_date()
        self.assertEqual(fd.fits_dict['L1PUBDAT'], '3014-08-03T00:00:00+00:00')

    def test_public_date_private_LCOEngineering(self):
        fd = FitsDict(File(self.fileobj), [], [])
        fd.fits_dict = {
            'PROPID': 'LCOEngineering',
            'DATE-OBS': '2016-04-01T00:00:00+00:00',
            'OBSTYPE': 'EXPOSE'
        }
        fd.set_public_date()
        self.assertEqual(fd.fits_dict['L1PUBDAT'], '3014-08-03T00:00:00+00:00')

    def test_public_date_exists(self):
        fd = FitsDict(File(self.fileobj), [], [])
        fd.fits_dict = {
            'PROPID': 'LCO2015',
            'DATE-OBS': '2099-04-01T00:00:00+00:00',
            'L1PUBDAT': '2099-04-01T00:00:00+00:00',
            'OBSTYPE': 'EXPOSE'
        }
        fd.set_public_date()
        self.assertEqual(fd.fits_dict['L1PUBDAT'], '2099-04-01T00:00:00+00:00')

    def test_normalize_related(self):
        fd = FitsDict(File(self.fileobj), [], [])
        fd.fits_dict = {
            'L1IDBIAS': 'bias_kb78_20151110_bin2x2',
            'L1IDFLAT': 'flat_kb78_20151106_SKYFLAT_bin2x2_V',
            'L1IDDARK': 'dark_kb78_20151110_bin2x2',
            'TARFILE': 'KEY2014A-002_0000476040_ftn_20160108_57396.tar.gz',
            'GUIDETAR': 'somekindoftarball.tar.gz'
        }
        fd.normalize_related()
        for key in fd.fits_dict:
            self.assertFalse(os.path.splitext(fd.fits_dict[key])[1])
