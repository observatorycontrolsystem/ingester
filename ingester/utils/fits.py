from astropy import wcs
import tarfile
import hashlib
import os

from ingester.exceptions import DoNotRetryError, RetryError


def get_fits_from_path(path):
    protocal_preface = 's3://'
    try:
        if 'tar.gz' in path:
            return get_meta_file_from_targz(path)
        elif path.startswith(protocal_preface):
            from ingester.s3 import S3Service
            return S3Service('').get_file(path)
        else:
            return open(path, 'rb')
    except FileNotFoundError as exc:
        raise RetryError(exc)


def get_md5(path):
    try:
        return hashlib.md5(open(path, 'rb').read()).hexdigest()
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
    for member in tf.getnames():
        if 'e00' in member or 'e90' in member:
            return tf.extractfile(member)
    raise DoNotRetryError('Spectral package missing meta fits!')


def wcs_corners_from_dict(fits_dict):
    """
    Take a fits dictionary and pick out the RA, DEC of each of the four corners.
    Then assemble a Polygon following the GeoJSON spec: http://geojson.org/geojson-spec.html#id4
    Note there are 5 positions. The last is the same as the first. We are defining lines,
    and you must close the polygon.
    """
    if any([fits_dict.get(k) is None for k in ['CD1_1', 'CD1_2', 'CD2_1', 'CD2_2']]) or \
            fits_dict.get('NAXIS3') is not None:
        # This file doesn't have sufficient information to provide an area
        return None

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
