from astropy.io import fits
from astropy import wcs


def fits_to_dict(f):
    hdulist = fits.open(f, mode='denywrite')
    full_dict = dict(hdulist[0].header)
    return full_dict


def wcs_corners_from_dict(fits_dict):
    w = wcs.WCS(fits_dict)
    sw = w.all_pix2world(1, 1, 1)
    sw = tuple([float(x) for x in sw])
    ne = w.all_pix2world(fits_dict['NAXIS1'], fits_dict['NAXIS2'], 1)
    ne = tuple([float(x) for x in ne])
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
