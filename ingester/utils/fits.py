from astropy.io import fits
from astropy import wcs
import numpy as np
from math import degrees


def fits_to_dict(f):
    hdulist = fits.open(f, mode='denywrite')
    full_dict = dict(hdulist[0].header)
    return full_dict


def wcs_corners_from_dict(fits_dict):
    """
    TODO:
    The following logic is  taken from
    https://github.com/LCOGT/satellite/blob/crosscorr/src/satellite/trails_cc.py#L181
    -1 left handed coordinates
    0 right handed coordinates
    """
    w = wcs.WCS(fits_dict)
    angle, parity = cd_to_rot(
        fits_dict['CD1_1'],
        fits_dict['CD1_2'],
        fits_dict['CD2_1'],
        fits_dict['CD2_2']
    )

    if parity == -1:
        pix_2_world_arg_sw = [fits_dict['NAXIS1'], 1, 1]
        pix_2_world_arg_ne = [1, fits_dict['NAXIS2'], 1]
    else:
        pix_2_world_arg_sw = [1, fits_dict['NAXIS2'], 1]
        pix_2_world_arg_ne = [fits_dict['NAXIS1'], 1, 1]

    if fits_dict.get('NAXIS3'):
        pix_2_world_arg_sw.append(1)
        pix_2_world_arg_ne.append(1)

    sw = w.all_pix2world(*pix_2_world_arg_sw)
    ne = w.all_pix2world(*pix_2_world_arg_ne)
    sw = tuple([float(x) for x in sw[0:2]])
    ne = tuple([float(x) for x in ne[0:2]])
    return (sw, ne)


def cd_to_rot(cd11, cd12, cd21, cd22):
    """
    Convert a CD matrix from a fits header to a rotation angle and parity
    (right vs left handed coordinates).
    :param cd11: CD1_1 header keyword
    :param cd12: CD1_2 header keyword
    :param cd21: CD2_1 header keyword
    :param cd22: CD2_2 header keyword
    :return posang: Position angle measured East of North
    :return xparity: 1.0 if right handed coordinate system, -1.0 if left handed
    The CD matrix is given by
    CD = scale * (  cos(posang)     sin(posang) ) ( xparity  0 )
                 ( -sin(posang)     cos(posang) ) (    0     1 )
    """

    # Figure out which direction the x-axis is pointing
    if np.abs(cd22) > 0:
        if cd11 / cd22 < 0:
            xparity = -1.0
            posang = degrees(np.arctan2(cd12, cd22))
        else:
            xparity = 1.0
            posang = degrees(np.arctan2(cd12, cd22))
    else:
        if cd12 / cd21 > 0:
            xparity = -1.0
            posang = degrees(np.arctan2(cd12, cd22))
        else:
            xparity = 1.0
            posang = degrees(np.arctan2(cd12, cd22))

    return posang, xparity


def remove_headers(dictionary, blacklist):
    for header in blacklist:
        try:
            del(dictionary[header])
        except KeyError:
            pass
    return dictionary


def missing_keys(dictionary, required):
    return [k for k in required if k not in dictionary]
