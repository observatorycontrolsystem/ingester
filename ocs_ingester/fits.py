import io
import logging
from datetime import timedelta

from astropy.io import fits
from dateutil.parser import parse

from ocs_ingester.exceptions import DoNotRetryError
from ocs_ingester.utils.fits import reduction_level, related_for_catalog, get_dayobs
from ocs_ingester.utils.fits import File
from ocs_ingester.settings import settings

logger = logging.getLogger('ocs_ingester')

LETTER_TO_OBSTYPE = {
    'b': 'BIAS',
    'd': 'DARK',
    'f': 'SKYFLAT',
    'g': 'GUIDE',
    's': 'STANDARD',
    'w': 'LAMPFLAT',
    'x': 'EXPERIMENTAL'
}

class FitsDict(object):
    INTEGER_TYPES = ['BLKUID', 'REQNUM', 'TRACKNUM', 'MOLUID']

    def __init__(self, file, required_headers, blacklist_headers):
        self.file = file
        self.required_headers = required_headers
        self.blacklist_headers = list(blacklist_headers)
        # Make sure to blacklist empty headers, since we never want them and they cause problems.
        if '' not in self.blacklist_headers:
            self.blacklist_headers.append('')

    def get_hdu_with_required_headers(self):
        with self.file.get_fits() as fits_file:
            with fits.open(io.BytesIO(fits_file.read()), mode='readonly') as hdulist:
                for hdu in hdulist:
                    fits_dict = dict(hdu.header)
                    if any([k for k in self.required_headers if k not in fits_dict]):
                        pass
                    else:
                        self.fits_dict = fits_dict
                        logger.info('Ingester extracted fits headers', extra={
                            'tags': {
                                'request_num': fits_dict.get('REQNUM'),
                                'PROPID': fits_dict.get('PROPID'),
                                'filename': '{}{}'.format(self.file.basename, self.file.extension)
                            }
                        })
                        return
                raise DoNotRetryError('Could not find required headers!')  # No headers met requirement

    def remove_blacklist_headers(self):
        for header in self.blacklist_headers:
            try:
                del(self.fits_dict[header])
            except KeyError:
                pass

    def normalize_null_values(self):
        #  Sometimes keywords use N/A to mean null
        for k, v in self.fits_dict.items():
            if v == 'N/A' or v == 'UNKNOWN' or v == 'UNSPECIFIED':
                if k in self.INTEGER_TYPES:
                    self.fits_dict[k] = None
                else:
                    self.fits_dict[k] = ''
            # Catch None and NONE values for Integer type fields so they pass archive validation
            elif k in self.INTEGER_TYPES and ('NONE' in str(v).upper()):
                self.fits_dict[k] = None

    def check_rlevel(self):
        if not self.fits_dict.get('RLEVEL'):
            # Check if the frame contains its reduction level, if not deduce it
            rlevel = reduction_level(self.file.basename, self.file.extension)
            self.fits_dict['RLEVEL'] = rlevel

    def check_catalog(self):
        if '_cat' in self.file.basename:
            if not self.fits_dict.get('L1IDCAT'):
                # Check if the catalog file contains it's target frame, if not deduce it
                l1idcat = related_for_catalog(self.file.basename)
                self.fits_dict['L1IDCAT'] = l1idcat
            # set obstype to CATALOG even though it's set to EXPOSE by the pipeline
            self.fits_dict['OBSTYPE'] = 'CATALOG'

    def check_dayobs(self):
        self.fits_dict['DAY-OBS'] = get_dayobs(self.fits_dict)

    def set_public_date(self):
        if not self.fits_dict.get('L1PUBDAT'):
            # Check if the frame doesnt specify a public date.
            if (self.fits_dict['OBSTYPE'] in settings.CALIBRATION_TYPES or
                    any([prop in self.fits_dict['PROPID'] for prop in settings.PUBLIC_PROPOSALS])):
                self.fits_dict['L1PUBDAT'] = self.fits_dict['DATE-OBS']
            elif (any([prop in self.fits_dict['PROPID'] for prop in settings.PRIVATE_PROPOSALS]) or
                    any([chars in self.file.basename for chars in settings.PRIVATE_FILE_TYPES])):
                # This should be private, set it to 999 years from DATE-OBS
                self.fits_dict['L1PUBDAT'] = (
                    parse(self.fits_dict['DATE-OBS']) + timedelta(days=365 * 999)
                ).isoformat()
            else:
                # This should be proprietary, set it to a year from DATE-OBS
                self.fits_dict['L1PUBDAT'] = (
                    parse(self.fits_dict['DATE-OBS']) + timedelta(days=365)
                ).isoformat()

    def round_exptime(self):
        """
        The science archive requires EXPTIME to have up to 13 digits,
        with up to 6 digits of precision after the decimal. We should
        enforce this in the ingester so we dont upload data we cannot ingest.
        """
        if self.fits_dict.get('EXPTIME'):
            self.fits_dict['EXPTIME'] = round(self.fits_dict['EXPTIME'], 6)

    def repair_obstype(self):
        if self.fits_dict.get('OBSTYPE', 'UNKNOWN').strip() == 'UNKNOWN':
            try:
                name_parts = self.file.basename.split('-')
                obstype_letter = name_parts[4][0]
                is_nres = 'igl' in self.fits_dict.get('ENCID', '') or 'igl' in self.fits_dict.get('TELID', '')
                if 'trace' == name_parts[0]:
                    obstype = 'TRACE'
                elif 'arc' == name_parts[0]:
                    obstype = 'ARC'
                elif 'bias' == name_parts[3]:
                    obstype = 'BIAS'
                elif 'bpm' == name_parts[3]:
                    obstype = 'BPM'
                elif obstype_letter == 'e':
                    if is_nres:
                        obstype = 'TARGET'
                    elif 'en' in name_parts[1]:
                        obstype = 'SPECTRUM'
                    else:
                        obstype = 'EXPOSE'
                elif obstype_letter == 'a':
                    if is_nres:
                        obstype = 'DOUBLE'
                    else:
                        obstype = 'ARC'
                elif obstype_letter in LETTER_TO_OBSTYPE:
                    obstype = LETTER_TO_OBSTYPE[obstype_letter]
                else:
                    # Failed to infer an OBSTYPE for this filename
                    raise Exception()
                # Set the OBSTYPE into the header
                self.fits_dict['OBSTYPE'] = obstype
            except Exception:
                raise DoNotRetryError('OBSTYPE is UNKNOWN and could not be inferred. Please manually correct OBSTYPE')

    def normalize_related(self):
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
            filename = self.fits_dict.get(key)
            if filename and filename != 'N/A':
                # The key has a value that isn't NA, we have a related frame
                basename, extension = File.get_basename_and_extension(filename)
                if extension:
                    # This value has an extention, so remove it.
                    self.fits_dict[key] = basename
                else:
                    pass
            else:
                # If the value is NA or the key doesn't exit, make sure it
                # is not the in dictionary we return
                try:
                    del self.fits_dict[key]
                except KeyError:
                    pass

    def as_dict(self):
        self.get_hdu_with_required_headers()
        self.remove_blacklist_headers()
        self.normalize_null_values()
        self.check_rlevel()
        self.check_catalog()
        self.check_dayobs()
        self.set_public_date()
        self.round_exptime()
        self.repair_obstype()
        self.normalize_related()
        return self.fits_dict
