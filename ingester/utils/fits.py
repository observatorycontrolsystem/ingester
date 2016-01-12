from astropy.io import fits
from astropy import wcs


def fits_to_dict(f):
    hdulist = fits.open(f, mode='denywrite')
    full_dict = dict(hdulist[0].header)
    return full_dict


def wcs_corners_from_dict(fits_dict):
    """
    Get the four corners of the image, and make a list of each
    ra and dec. Then take the max RA and Max dec, that is the NE
    corner. Min RA and Min dec is SW.
    """
    if fits_dict.get('NAXIS3'):
        # only used in spectral targets
        return None

    w = wcs.WCS(fits_dict)
    corners = w.all_pix2world(
        [
            (1, 1),
            (1, fits_dict['NAXIS2']),
            (fits_dict['NAXIS1'], fits_dict['NAXIS2']),
            (fits_dict['NAXIS1'], 1),
        ],
        1
    )
    ras = [x[0] for x in corners]
    decs = [x[1] for x in corners]
    sw = (min(ras), min(decs))
    ne = (max(ras), max(decs))

    return (sw, ne)


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
