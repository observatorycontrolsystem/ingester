import unittest
import os
from ingester.utils.fits import normalize_related


class TestFitsUtils(unittest.TestCase):

    def test_normalize_related(self):
        fits_dict = {
            'L1IDBIAS': 'bias_kb78_20151110_bin2x2',
            'L1IDFLAT': 'flat_kb78_20151106_SKYFLAT_bin2x2_V',
            'L1IDDARK': 'dark_kb78_20151110_bin2x2',
            'TARFILE': 'KEY2014A-002_0000476040_ftn_20160108_57396.tar.gz'
        }
        fits_dict = normalize_related(fits_dict)
        for key in fits_dict:
            self.assertTrue(os.path.splitext(fits_dict[key])[1])
