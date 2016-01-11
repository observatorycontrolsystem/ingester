from astropy.io import fits
from astropy import wcs


def fits_to_dict(f):
    hdulist = fits.open(f, mode='denywrite')
    full_dict = dict(hdulist[0].header)
    return full_dict


def wcs_corners_from_dict(fits_dict):
    """
    TODO:
    Discuss with scientists the corret way to
    get these coordinates
    """
    w = wcs.WCS(fits_dict)
    pix_2_world_arg_sw = [fits_dict['NAXIS1'], 1, 1]
    pix_2_world_arg_ne = [1, fits_dict['NAXIS2'], 1]

    if fits_dict.get('NAXIS3') is not None:
        pix_2_world_arg_sw.append(1)
        pix_2_world_arg_ne.append(1)

    sw = w.all_pix2world(*pix_2_world_arg_sw)
    ne = w.all_pix2world(*pix_2_world_arg_ne)
    sw = tuple([float(x) for x in sw[0:2]])
    ne = tuple([float(x) for x in ne[0:2]])
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
