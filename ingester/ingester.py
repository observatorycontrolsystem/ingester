from ingester.fits import FitsDict
from ingester.exceptions import BackoffRetryError
from ingester.utils.fits import get_basename_and_extension, wcs_corners_from_dict, get_md5


class Ingester(object):
    """ Ingester - ingest a single file into the archive
    A single instance of this class is responsbile for parsing a fits file,
    uploading the data to s3, and making a call to the archive api.
    """

    def __init__(self, path, s3_service, archive_service, required_headers=None, blacklist_headers=None):
        self.path = path
        self.s3_service = s3_service
        self.archive_service = archive_service
        self.required_headers = required_headers if required_headers else []
        self.blacklist_headers = blacklist_headers if blacklist_headers else []

    def ingest(self):
        self.basename, self.extension = get_basename_and_extension(self.path)

        # Get the Md5 checksum of this file and check if it already exists in the archive
        md5 = get_md5(self.path)
        self.archive_service.check_for_existing_version(md5)

        # Transform this fits file into a cleaned dictionary
        fits_dict = FitsDict(self.path, self.required_headers, self.blacklist_headers).as_dict()

        # Upload the file to s3 and get version information back
        version = self.s3_service.upload_file(self.path)

        # Make sure our md5 matches amazons
        if version['md5'] != md5:
            raise BackoffRetryError('S3 md5 did not match ours')

        # Construct final archive payload and post to archive
        fits_dict['area'] = wcs_corners_from_dict(fits_dict)
        fits_dict['version_set'] = [version]
        fits_dict['basename'] = self.basename
        self.archive_service.post_frame(fits_dict)
