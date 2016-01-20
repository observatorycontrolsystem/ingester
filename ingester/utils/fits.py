from astropy.io import fits
from astropy import wcs
from ingester.exceptions import DoNotRetryError
import tarfile
import dateutil
from datetime import timedelta


def get_meta_file_from_targz(f):
    tf = tarfile.open(fileobj=f)
    for member in tf.getnames():
        if 'e00.fits' in member:
            return tf.extractfile(member)
    raise DoNotRetryError('Spectral package missing meta fits!')


def fits_to_dict(f):
    hdulist = fits.open(f, mode='denywrite')
    full_dict = dict(hdulist[0].header)
    f.seek(0)
    return full_dict


def add_required_headers(filename, fits_dict):
    # TODO: Remove this function entirely. We need these for now
    # because the pipeline does not write them as headers
    if not fits_dict.get('RLEVEL'):
        rlevel = reduction_level(filename)
        fits_dict['RLEVEL'] = rlevel
    if filename.endswith('_cat.fits') and not fits_dict.get('L1IDCAT'):
        l1idcat = related_for_catalog(filename)
        fits_dict['L1IDCAT'] = l1idcat
    if not fits_dict.get('L1PUBDAT') and fits_dict['RLEVEL'] > 0:
        fits_dict['L1PUBDAT'] = str(
            dateutil.parser.parse(fits_dict['DATE-OBS']) + timedelta(days=365)
        )
    return fits_dict


def wcs_corners_from_dict(fits_dict):
    """
    Take a fits dictionary and pic out the RA, DEC of each of the four corners.
    Then assemble a Polygon following the GeoJSON spec: http://geojson.org/geojson-spec.html#id4
    Note there are 5 positions. The last is the same as the first. We are defining lines,
    and you must close the polygon.
    """
    if any([fits_dict.get(k) is None for k in ['CD1_1', 'CD1_2', 'CD2_1', 'CD2_2']]) or \
            fits_dict.get('NAXIS3') is not None:
        # This file doesn't have sufficient information to provide an area
        return None
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


def missing_keys(dictionary, required):
    return [k for k in required if k not in dictionary]


def reduction_level(filename):
    # TODO: Pipeline should write this value instead of
    # being inferred from the filename
    if filename.endswith('tar.gz'):
        return 90
    else:
        try:
            return int(filename[-7:-5])
        except ValueError:
            return 0


def related_for_catalog(filename):
    # TODO: Pipeline should write this value instead of
    # being inferred from the filename
    return filename.replace('_cat', '').replace('.fits', '')
