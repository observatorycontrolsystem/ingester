import unittest
import os

from ingester.fits import FitsDict


class TestFits(unittest.TestCase):
    def test_null_values(self):
        fd = FitsDict('', [], [])
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
        fd = FitsDict('', [], ['FOO'])
        fd.fits_dict = {
            'FOO': 'BAR',
            'BAZ': 'BAM'
        }
        fd.remove_blacklist_headers()
        self.assertNotIn('FOO', fd.fits_dict.keys())
        self.assertIn('BAZ', fd.fits_dict.keys())

    def test_rlevel_present(self):
        fd = FitsDict('', [], [])
        fd.fits_dict = {'RLEVEL': 91}
        fd.check_rlevel()
        self.assertEqual(91, fd.fits_dict['RLEVEL'])

    def test_rlevel_missing(self):
        fd = FitsDict('', [], [])
        fd.fits_dict = {}
        fd.basename = 'something-e11'
        fd.check_rlevel()
        self.assertEqual(11, fd.fits_dict['RLEVEL'])

    def test_catalog_file(self):
        fd = FitsDict('', [], [])
        fd.fits_dict = {}
        fd.basename = 'something-e90_cat'
        fd.check_catalog()
        self.assertEqual('something-e90', fd.fits_dict['L1IDCAT'])

    def test_public_date_public_file(self):
        fd = FitsDict('', [], [])
        fd.fits_dict = {
            'PROPID': 'EPOTHING',
            'DATE-OBS': '2016-04-01T00:00:00+00:00',
            'OBSTYPE': 'EXPOSE'
        }
        fd.set_public_date()
        self.assertEqual(fd.fits_dict['L1PUBDAT'], fd.fits_dict['DATE-OBS'])

    def test_public_date_private_file(self):
        fd = FitsDict('', [], [])
        fd.fits_dict = {
            'PROPID': 'LCO2015',
            'DATE-OBS': '2016-04-01T00:00:00+00:00',
            'OBSTYPE': 'EXPOSE'
        }
        fd.set_public_date()
        self.assertEqual(fd.fits_dict['L1PUBDAT'], '2017-04-01T00:00:00+00:00')

    def test_public_date_exists(self):
        fd = FitsDict('', [], [])
        fd.fits_dict = {
            'PROPID': 'LCO2015',
            'DATE-OBS': '2099-04-01T00:00:00+00:00',
            'L1PUBDAT': '2099-04-01T00:00:00+00:00',
            'OBSTYPE': 'EXPOSE'
        }
        fd.set_public_date()
        self.assertEqual(fd.fits_dict['L1PUBDAT'], '2099-04-01T00:00:00+00:00')

    def test_normalize_related(self):
        fd = FitsDict('', [], [])
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
