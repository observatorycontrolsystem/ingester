import unittest
import os
from ingester import Ingester, HEADER_BLACKLIST
from utils import fits_to_dict

FITS_PATH = os.path.join(
    os.path.dirname(__file__),
    'fits/coj1m011-kb05-20150219-0125-e90.fits'
)


class TestUtils(unittest.TestCase):
    def test_fits_to_dict(self):
        result = fits_to_dict(FITS_PATH, HEADER_BLACKLIST)
        for header in HEADER_BLACKLIST:
            self.assertNotIn(
                header,
                result.keys()
            ),
        self.assertEqual(
            'coj1m011-kb05-20150219-0125-e00.fits',
            result['ORIGNAME']
        )


class TestIngester(unittest.TestCase):
    def setUp(self):
        self.ingester = Ingester('http://localhost', 'fakebucket')
