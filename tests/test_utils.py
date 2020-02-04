import io
import hashlib
import unittest
from lco_ingester.utils.fits import File, reduction_level, get_dayobs


class TestFitsUtils(unittest.TestCase):

    def test_get_basename_and_extension(self):
        path = '/archive/coj/kb84/20160325/raw/coj0m405-kb84-20160325-0095-e00.fits'
        basename, extension = File.get_basename_and_extension(path)
        self.assertEqual(basename, 'coj0m405-kb84-20160325-0095-e00')
        self.assertEqual(extension, '.fits')

        path = '/archive/coj/kb84/20160325/raw/coj0m405-kb84-20160325-0095-e00.fits.fz'
        basename, extension = File.get_basename_and_extension(path)
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

    def test_get_file_length(self):
        with io.BytesIO(b'1234567890') as fileobj:
            file = File(fileobj, 'some_fits.fits.fz')
            self.assertEqual(len(file), 10)

    def test_get_file_from_start(self):
        with io.BytesIO(b'1234567890') as fileobj:
            fileobj.read()
            file = File(fileobj, 'some_fits.fits.fz')
            file.fileobj.read()  # Go to end of file
            self.assertEqual(file.fileobj.tell(), 10)
            self.assertEqual(file.get_from_start().tell(), 0)

    def test_md5(self):
        # Make sure that md5 is computed from the start of the fileobj, even after having been read
        bstring = b'1234567890'
        md5bstring = hashlib.md5(bstring).hexdigest()
        with io.BytesIO(b'1234567890') as fileobj:
            file = File(fileobj, run_validate=False)
            md51 = file.get_md5()
            file.fileobj.read()
            md52 = file.get_md5()
            self.assertEqual(md51, md52)
            self.assertEqual(md5bstring, md51)

    def test_get_dayobs_no_dayobs(self):
        fits_dict = {'DATE-OBS': '2020-01-31T20:09:56.956'}
        self.assertEqual('20200131', get_dayobs(fits_dict))

    def test_get_dayobs(self):
        fits_dict = {'DAY-OBS': '20200131'}
        self.assertEqual('20200131', get_dayobs(fits_dict))

