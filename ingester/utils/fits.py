from contextlib import contextmanager
from opentsdb_python_metrics.metric_wrappers import metric_timer
from dateutil.relativedelta import relativedelta
from dateutil.parser import parse
from astropy import wcs
from datetime import datetime, timedelta
import tarfile
import hashlib
import os

from ingester.exceptions import DoNotRetryError, RetryError


@contextmanager
def reset_file(fileobj):
    """
    Use as a context manager to ensure that a file-like object is reset to the
    initial position before and after a `with` block. This is useful if you need
    the entire file to be read, but the reader does not reset it.
    """
    fileobj.seek(0)
    try:
        yield
    finally:
        fileobj.seek(0)


@metric_timer('ingester.get_fits')
def get_fits_from_path(path):
    protocol_preface = 's3://'
    try:
        if 'tar.gz' in path:
            return get_meta_file_from_targz(path)
        elif path.startswith(protocol_preface):
            from ingester.s3 import S3Service
            return S3Service('').get_file(path)
        else:
            return open(path, 'rb')
    except FileNotFoundError as exc:
        raise RetryError(exc)


@metric_timer('ingester.get_md5')
def get_md5(fileobj):
    try:
        with reset_file(fileobj):
            return hashlib.md5(fileobj.read()).hexdigest()
    except FileNotFoundError as exc:
        raise RetryError(exc)


def get_basename_and_extension(path):
    filename = os.path.basename(path)
    if filename.find('.') > 0:
        basename = filename[:filename.index('.')]
        extension = filename[filename.index('.'):]
    else:
        basename = filename
        extension = ''
    return basename, extension


def get_meta_file_from_targz(path):
    tf = tarfile.open(path, 'r')
    for member in tf.getmembers():
        if any(x + '.fits' in member.name for x in ['e00', 'e90', 'e91']) and member.isfile():
            return tf.extractfile(member)
    raise DoNotRetryError('Spectral package missing meta fits!')


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


def wcs_corners_from_dict(fits_dict):
    """
    Take a fits dictionary and pick out the RA, DEC of each of the four corners.
    Then assemble a Polygon following the GeoJSON spec: http://geojson.org/geojson-spec.html#id4
    Note there are 5 positions. The last is the same as the first. We are defining lines,
    and you must close the polygon.

    If this is a spectrograph (NRES only at the moment) then construct it out of the ra/dec
    and radius.
    """
    if fits_dict.get('RADIUS'):
        ra = fits_dict['RA']
        dec = fits_dict['DEC']
        r = fits_dict['RADIUS']

        radius_in_degrees = r/3600.0
        ra_in_degrees = ra * 15.0

        c1 = (ra_in_degrees - radius_in_degrees, dec + radius_in_degrees)
        c2 = (ra_in_degrees + radius_in_degrees, dec + radius_in_degrees)
        c3 = (ra_in_degrees + radius_in_degrees, dec - radius_in_degrees)
        c4 = (ra_in_degrees - radius_in_degrees, dec - radius_in_degrees)

    elif any([fits_dict.get(k) is None for k in ['CD1_1', 'CD1_2', 'CD2_1', 'CD2_2', 'NAXIS1', 'NAXIS2']]) or \
            fits_dict.get('NAXIS3') is not None:
        # This file doesn't have sufficient information to provide an area
        return None

    else:
        # Find the RA and Dec coordinates of all 4 corners of the image
        w = wcs.WCS(fits_dict)
        c1 = w.all_pix2world(1, 1, 1)
        c2 = w.all_pix2world(1, fits_dict['NAXIS2'], 1)
        c3 = w.all_pix2world(fits_dict['NAXIS1'], fits_dict['NAXIS2'], 1)
        c4 = w.all_pix2world(fits_dict['NAXIS1'], 1, 1)

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

    # if the observation was more than 6 months ago, this is someone
    # uploading older data, and it can skip straight to STANDARD_IA
    if dateobs < (datetime.utcnow() + relativedelta(months=-6)):
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
