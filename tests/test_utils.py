import unittest
from ingester.utils.fits import get_basename_and_extension, reduction_level


class TestFitsUtils(unittest.TestCase):

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
