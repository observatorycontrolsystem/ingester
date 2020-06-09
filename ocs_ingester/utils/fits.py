from datetime import datetime, timedelta
from contextlib import contextmanager
import tarfile
import hashlib
import os

from dateutil.parser import parse
from astropy import wcs

from ocs_ingester.exceptions import DoNotRetryError
from ocs_ingester.utils import metrics


class File:
    """
    Wrapper class to help pass around file objects.

    The user of this class is expected to close the file object that is passed in.
    """
    def __init__(self, fileobj, path=None, run_validate=True):
        self.fileobj = fileobj
        self.path = path
        self.basename, self.extension = self.get_basename_and_extension(self.filename)
        if run_validate:
            self.validate()

    def get_from_start(self):
        self.fileobj.seek(0)
        return self.fileobj

    @contextmanager
    def get_fits(self):
        """
        Return the fits file associated with this file. Generally this just the fileobj itself,
        but certain spectral data must have their fits files extracted. Use this as a context
        manager.
        """
        fits_file, tar_file = None, None
        try:
            if 'tar.gz' in self.filename:
                tar_file = tarfile.open(fileobj=self.get_from_start(), mode='r')
                fits_file = self._get_meta_file_from_targz(tar_file)
            else:
                fits_file = self.get_from_start()

            yield fits_file

        finally:
            if tar_file and fits_file:
                # The fits file object came from the tar file extraction, and must be closed
                fits_file.close()
            if tar_file:
                tar_file.close()

    @staticmethod
    def _get_meta_file_from_targz(tarfileobj):
        for member in tarfileobj.getmembers():
            if any(x + '.fits' in member.name for x in ['e00', 'e90', 'e91']) and member.isfile():
                return tarfileobj.extractfile(member)
        raise DoNotRetryError('Spectral package missing meta fits!')

    @metrics.method_timer('ingester.get_md5')
    def get_md5(self):
        return hashlib.md5(self.get_from_start().read()).hexdigest()

    @property
    def filename(self):
        # The filename, can be a full path or at least the basename plus the extension
        filename = None
        if self.path is not None:
            filename = self.path
        elif hasattr(self.fileobj, 'name'):
            filename = self.fileobj.name
        elif hasattr(self.fileobj, 'filename'):
            filename = self.fileobj.filename
        return filename

    @staticmethod
    def get_basename_and_extension(path):
        basename, extension = None, None
        if path is not None:
            filename = os.path.basename(path)
            if filename.find('.') > 0:
                basename = filename[:filename.index('.')]
                extension = filename[filename.index('.'):]
            else:
                basename = filename
                extension = ''
        return basename, extension

    def __len__(self):
        self.fileobj.seek(0, os.SEEK_END)
        length = self.fileobj.tell()
        self.fileobj.seek(0)
        return length

    def validate(self):
        if self.filename is None:
            raise DoNotRetryError('Unable to get filename from file object, must specify a path')


def obs_end_time_from_dict(fits_dict):
    dateobs = parse(fits_dict['DATE-OBS'])
    if fits_dict.get('UTSTOP'):
        # UTSTOP is just a time - we need the date as well to be sure when this is
        utstop_time = parse(fits_dict['UTSTOP'])
        utstop_date = dateobs.date()
        if abs(dateobs.hour - utstop_time.hour) > 12:
            # There was a date rollover during this observation, so set the date for utstop
            utstop_date += timedelta(days=1)

        return datetime.combine(utstop_date, utstop_time.time())
    elif fits_dict.get('EXPTIME'):
        return dateobs + timedelta(seconds=fits_dict['EXPTIME'])
    return dateobs


def _values_are_set(fits_dict, headers):
    """Check that the values for the provided headers are set"""
    empty_values = [None, '']
    values = [fits_dict.get(header) for header in headers]
    return all([value not in empty_values for value in values])


def wcs_corners_from_dict(fits_dict):
    """
    Take a fits dictionary and pick out the RA, DEC of each of the four corners.
    Then assemble a Polygon following the GeoJSON spec: http://geojson.org/geojson-spec.html#id4
    Note there are 5 positions. The last is the same as the first. We are defining lines,
    and you must close the polygon.

    If this is a spectrograph (NRES only at the moment) then construct it out of the ra/dec
    and radius.
    """
    if _values_are_set(fits_dict, ['RADIUS', 'RA', 'DEC']):
        ra = fits_dict['RA']
        dec = fits_dict['DEC']
        r = fits_dict['RADIUS']

        radius_in_degrees = r / 3600.0
        ra_in_degrees = ra * 15.0

        c1 = (ra_in_degrees - radius_in_degrees, dec + radius_in_degrees)
        c2 = (ra_in_degrees + radius_in_degrees, dec + radius_in_degrees)
        c3 = (ra_in_degrees + radius_in_degrees, dec - radius_in_degrees)
        c4 = (ra_in_degrees - radius_in_degrees, dec - radius_in_degrees)

    elif (
            _values_are_set(fits_dict, ['CD1_1', 'CD1_2', 'CD2_1', 'CD2_2', 'NAXIS1', 'NAXIS2'])
            and not _values_are_set(fits_dict, ['NAXIS3'])
    ):
        # Find the RA and Dec coordinates of all 4 corners of the image
        w = wcs.WCS(fits_dict)
        c1 = w.all_pix2world(1, 1, 1)
        c2 = w.all_pix2world(1, fits_dict['NAXIS2'], 1)
        c3 = w.all_pix2world(fits_dict['NAXIS1'], fits_dict['NAXIS2'], 1)
        c4 = w.all_pix2world(fits_dict['NAXIS1'], 1, 1)

    else:
        # This file doesn't have sufficient information to provide an area
        return None

    return {
        'type': 'Polygon',
        'coordinates': [
            [
                [
                    float(c1[0]),
                    float(c1[1])
                ],
                [
                    float(c2[0]),
                    float(c2[1])
                ],
                [
                    float(c3[0]),
                    float(c3[1])
                ],
                [
                    float(c4[0]),
                    float(c4[1])
                ],
                [
                    float(c1[0]),
                    float(c1[1])
                ]
            ]
        ]
    }


def get_storage_class(fits_dict):
    # date of the observation, from the FITS headers
    dateobs = parse(fits_dict['DATE-OBS'])

    # if the observation was more than 60 days ago, this is someone
    # uploading older data, and it can skip straight to STANDARD_IA
    if dateobs < (datetime.utcnow() - timedelta(days=60)):
        return 'STANDARD_IA'

    # everything else goes into the STANDARD storage class, and will
    # be switched to STANDARD_IA by S3 Lifecycle Rules
    return 'STANDARD'


def reduction_level(basename, extension):
    # TODO: Pipeline should write this value instead of
    # being inferred from the filename
    MAX_REDUCTION = 90
    MIN_REDUCTION = 0

    # remove the _cat extension from catalog files
    basename = basename.replace('_cat', '')

    if extension == '.tar.gz':
        return MAX_REDUCTION
    else:
        try:
            # lsc1m005-kb78-20151007-0214-x00
            # extract reduction level at the position of 00
            return int(basename[-2:])
        except ValueError:
            # Some filenames don't have this extension - return a sensible default
            return MIN_REDUCTION


def related_for_catalog(basename):
    # TODO: Pipeline should write this value instead of
    # being inferred from the filename
    # a file's corresponding catalog is that filename + _cat
    return basename.replace('_cat', '')


def get_dayobs(fits_dict: dict):
    """
    Get dayobs from a fits_dict
    :param fits_dict: Fits dictionary with required header keywords
    :return: Date in DAY-OBS YYYYMMDD format
    """
    day_obs = fits_dict.get('DAY-OBS')
    if not day_obs:
        # Fall back to DATE-OBS
        date_obs = fits_dict.get('DATE-OBS')
        day_obs = date_obs.split('T')[0].replace('-', '')

    return day_obs


