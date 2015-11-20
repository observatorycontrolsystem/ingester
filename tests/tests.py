import unittest
import os
from ingester import Ingester


class TestIngester(unittest.TestCase):
    def setUp(self):
        self.path = os.path.join(
            os.path.dirname(__file__),
            'fits/1.fits'
        )
        self.ingester = Ingester('http://localhost', 'fakebucket')

    def test_fits_to_dict(self):
        result = Ingester.fits_to_dict(self.path)
        self.assertEqual(
            'coj1m011-kb05-20150219-0125-e00.fits',
            result['ORIGNAME']
        )
