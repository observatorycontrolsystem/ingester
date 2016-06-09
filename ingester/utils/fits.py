from astropy.io import fits
from astropy import wcs
from ingester.exceptions import DoNotRetryError
import tarfile
from dateutil.parser import parse
import os
from datetime import timedelta

CALIBRATION_TYPES = ['BIAS', 'DARK', 'SKYFLAT', 'EXPERIMENTAL']
PUBLIC_PROPOSALS = ['EPO', 'calib', 'standard']
INTEGER_TYPES = ['BLKUID', 'REQNUM']


def get_basename_and_extension(path):
    filename = os.path.basename(path)
    if filename.find('.') > 0:
        basename = filename[:filename.index('.')]
        extension = filename[filename.index('.'):]
    else:
        basename = filename
        extension = ''
    return basename, extension


def get_meta_file_from_targz(f):
    #  f is an already open fileobj
    tf = tarfile.open(fileobj=f)
    for member in tf.getnames():
        if 'e00' in member or 'e90' in member:
            return tf.extractfile(member)
    raise DoNotRetryError('Spectral package missing meta fits!')


def fits_to_dict(f, required_keys, blacklist_headers):
    hdulist = fits.open(f, mode='denywrite')
    for hdu in hdulist:
        fits_dict = dict(hdu.header)
        if any([k for k in required_keys if k not in fits_dict]):
            pass
        else:
            return remove_headers(fits_dict, blacklist_headers)
    raise DoNotRetryError('Could not find required headers!')


def normalize_null_values(fits_dict):
    #  Sometimes keywords use N/A to mean null
    for k, v in fits_dict.items():
        if v == 'N/A' or v == 'UNKNOWN' or v == 'UNSPECIFIED':
            if k in INTEGER_TYPES:
                fits_dict[k] = None
            else:
                fits_dict[k] = ''
    return fits_dict


def add_required_headers(basename, extension, fits_dict):
    # TODO: Remove this function entirely. We need these for now
    # because the pipeline does not write them as headers
    if not fits_dict.get('RLEVEL'):
        # Check if the frame contains its reduction level, if not deduce it
        rlevel = reduction_level(basename, extension)
        fits_dict['RLEVEL'] = rlevel
    if '_cat' in basename:
        if not fits_dict.get('L1IDCAT'):
            # Check if the catalog file contains it's target frame, if not deduce it
            l1idcat = related_for_catalog(basename)
            fits_dict['L1IDCAT'] = l1idcat
        # set obstype to CATALOG even though it's set to EXPOSE by the pipeline
        fits_dict['OBSTYPE'] = 'CATALOG'
    if not fits_dict.get('L1PUBDAT'):
        # Check if the frame doesnt specify a public date.
        if (fits_dict['OBSTYPE'] in CALIBRATION_TYPES or
                any([prop in fits_dict['PROPID'] for prop in PUBLIC_PROPOSALS])):
            fits_dict['L1PUBDAT'] = fits_dict['DATE-OBS']
        else:
            # This should be proprietarty, set it to a year from DATE-OBS
            fits_dict['L1PUBDAT'] = (
                parse(fits_dict['DATE-OBS']) + timedelta(days=365)
            ).isoformat()
    return fits_dict


def normalize_related(fits_dict):
    """
    Fits files contain several keys whose values are the filenames
    of frames that they are related to.
    Sometimes the keys are non-existant, sometimes they contain
    'N/A' to represent null, sometimes they contain filenames with
    the file extension appended, sometimes without an extension.
    This function attempts to normalize these values to not exist
    if the value should be null, or if they do, be the filename
    without extension.
    """
    related_frame_keys = [
        'L1IDBIAS', 'L1IDDARK', 'L1IDFLAT', 'L1IDSHUT',
        'L1IDMASK', 'L1IDFRNG', 'L1IDCAT', 'TARFILE',
        'ORIGNAME', 'ARCFILE', 'FLATFILE', 'GUIDETAR'
    ]
    for key in related_frame_keys:
        filename = fits_dict.get(key)
        if filename and filename != 'N/A':
            # The key has a value that isn't NA, we have a related frame
            basename, extension = get_basename_and_extension(filename)
            if extension:
                # This value has an extention, so remove it.
                fits_dict[key] = basename
            else:
                pass
        else:
            # If the value is NA or the key doesn't exit, make sure it
            # is not the in dictionary we return
            try:
                del fits_dict[key]
            except KeyError:
                pass
    return fits_dict


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


def remove_headers(dictionary, blacklist):
    for header in blacklist:
        try:
            del(dictionary[header])
        except KeyError:
            pass
    return dictionary


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
