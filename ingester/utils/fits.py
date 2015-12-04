from astropy.io import fits


def fits_to_dict(path, blacklist=[]):
    hdulist = fits.open(path)
    full_dict = dict(hdulist[0].header)
    for header in blacklist:
        del(full_dict[header])
    return full_dict
