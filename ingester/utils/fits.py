from astropy.io import fits
from astropy import wcs


def fits_to_dict(f):
    hdulist = fits.open(f, mode='denywrite')
    full_dict = dict(hdulist[0].header)
    return full_dict


def wcs_corners_from_dict(fits_dict):
    """
    Take a fits dictionary and pic out the RA, DEC of each of the four corners.
    Then assemble a Polygon following the GeoJSON spec: http://geojson.org/geojson-spec.html#id4
    Note there are 5 positions. The last is the same as the first. We are defining lines,
    and you must close the polygon.
    """
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
    try:
        rlevel = int(filename[-7:-5])
    except ValueError:
        rlevel = 0
    return rlevel


def related_for_catalog(filename):
    # TODO: Pipeline should write this value instead of
    # being inferred from the filename
    return filename.replace('_cat', '').replace('.fits', '')
