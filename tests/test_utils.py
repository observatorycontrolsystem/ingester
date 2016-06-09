import unittest
import os
from ingester.utils.fits import (normalize_related, normalize_null_values,
                                 get_basename_and_extension, reduction_level)


class TestFitsUtils(unittest.TestCase):

    def test_normalize_related(self):
        fits_dict = {
            'L1IDBIAS': 'bias_kb78_20151110_bin2x2',
            'L1IDFLAT': 'flat_kb78_20151106_SKYFLAT_bin2x2_V',
            'L1IDDARK': 'dark_kb78_20151110_bin2x2',
            'TARFILE': 'KEY2014A-002_0000476040_ftn_20160108_57396.tar.gz',
            'GUIDETAR': 'somekindoftarball.tar.gz'
        }
        fits_dict = normalize_related(fits_dict)
        for key in fits_dict:
            self.assertFalse(os.path.splitext(fits_dict[key])[1])

    def test_normalize_null(self):
        fits_dict = {
            'OBJECT': 'UNKNOWN',
            'PROPID': 'N/A',
            'BLKUID': 'N/A',
            'INSTRUME': 'fl03',
            'REQNUM': 'UNSPECIFIED'
        }

        normalized = normalize_null_values(fits_dict)
        self.assertEqual('', normalized['OBJECT'])
        self.assertEqual('', normalized['PROPID'])
        self.assertIsNone(normalized['BLKUID'])
        self.assertEqual('fl03', normalized['INSTRUME'])
        self.assertIsNone(normalized['REQNUM'])

    def test_get_basename_and_extension(self):
        path = '/archive/coj/kb84/20160325/raw/coj0m405-kb84-20160325-0095-e00.fits'
        basename, extension = get_basename_and_extension(path)
        self.assertEqual(basename, 'coj0m405-kb84-20160325-0095-e00')
        self.assertEqual(extension, '.fits')

        path = '/archive/coj/kb84/20160325/raw/coj0m405-kb84-20160325-0095-e00.fits.fz'
        basename, extension = get_basename_and_extension(path)
        self.assertEqual(basename, 'coj0m405-kb84-20160325-0095-e00')
        self.assertEqual(extension, '.fits.fz')

    def test_reduction_level(self):
        basename = 'coj1m003-kb71-20160326-0063-e90'
        extension = '.fits.fz'
        self.assertEqual(reduction_level(basename, extension), 90)

        basename = 'coj1m003-kb71-20160326-0063-e00'
        self.assertEqual(reduction_level(basename, extension), 0)

        basename = 'somecrazyfloydspackage'
        extension = '.tar.gz'
        self.assertEqual(reduction_level(basename, extension), 90)

        basename = 'coj1m003-kb71-20160326-0063-e10_cat'
        extension = '.fits.fz'
        self.assertEqual(reduction_level(basename, extension), 10)
