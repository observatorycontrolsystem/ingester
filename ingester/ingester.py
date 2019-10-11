from ingester.fits import FitsDict
from ingester.exceptions import BackoffRetryError, NonFatalDoNotRetryError
from ingester.utils.fits import get_basename_and_extension
from ingester.utils.fits import wcs_corners_from_dict
from ingester.utils.fits import get_storage_class
from ingester.utils.fits import get_md5
from ingester.archive import ArchiveService
from ingester.s3 import S3Service
from settings import settings


def frame_exists(path, **kwargs):
    """
    Checks if the frame exists in the archive.

    :param path:
    :return:
    """
    api_root = kwargs.get('api_root') or settings.API_ROOT
    auth_token = kwargs.get('auth_token') or settings.AUTH_TOKEN
    archive = ArchiveService(api_root=api_root, auth_token=auth_token)
    md5 = get_md5(path)
    return archive.version_exists(md5)


def validate_fits_and_create_archive_record(path, **kwargs):
    """
    Validate the fits file and also create an archive record from it.

    After this step the version would still be missing

    Returns the constructed record

    :param path:
    :return:
    """
    required_headers = kwargs.get('required_headers') or settings.REQUIRED_HEADERS
    blacklist_headers = kwargs.get('blacklist_headers') or settings.HEADER_BLACKLIST
    json_record = FitsDict(path, required_headers, blacklist_headers).as_dict()
    basename, _ = get_basename_and_extension(path)
    json_record['area'] = wcs_corners_from_dict(json_record)
    json_record['basename'] = basename
    return json_record


def upload_file_to_s3(path, **kwargs):
    """
    Uploads a file to s3.

    :param path: Path to file
    :param kwargs: Other keyword arguments
    :return: Version information for the file that was uploaded
    """
    # TODO: Add transfer acceleration option.
    bucket = kwargs.get('bucket') or settings.BUCKET
    storage_class = kwargs('storage_class') or 'STANDARD'
    s3 = S3Service(bucket)
    # Returns the version, which holds in it the md5 that was uploaded
    return s3.upload_file(path, storage_class)


def ingest_archive_record(version, record, **kwargs):
    """
    Ingest an archive record.

    :param version: Result of the upload to s3
    :param record: Archive record to ingest
    :param kwargs: Other keyword arguments
    :return: The archive record that was ingested
    """
    api_root = kwargs.get('api_root') or settings.API_ROOT
    auth_token = kwargs.get('auth_token') or settings.AUTH_TOKEN
    archive = ArchiveService(api_root=api_root, auth_token=auth_token)
    # Construct final archive payload and post to archive
    record['version_set'] = [version]
    result = archive.post_frame(record)
    # Add some useful information from the result
    record['frameid'] = result.get('id')
    record['filename'] = result.get('filename')
    return record


def upload_file_and_ingest_to_archive(path, **kwargs):
    """
    Ingest and upload a file.

    Includes safety checks and the option to record metrics for various steps.

    :param path: Path to file
    :param kwargs: Other keyword arguments
    :return: Information about the uploaded file and record
    """
    required_headers = kwargs.get('required_headers') or settings.REQUIRED_HEADERS
    blacklist_headers = kwargs.get('blacklist_headers') or settings.HEADER_BLACKLIST
    api_root = kwargs.get('api_root') or settings.API_ROOT
    auth_token = kwargs.get('auth_token') or settings.AUTH_TOKEN
    bucket = kwargs.get('bucket') or settings.BUCKET
    archive = ArchiveService(api_root=api_root, auth_token=auth_token)
    s3 = S3Service(bucket)
    ingester = Ingester(path, s3, archive, required_headers, blacklist_headers)
    return ingester.ingest()


class Ingester(object):
    """
    Ingest a single file into the archive.

    A single instance of this class is responsible for parsing a fits file,
    uploading the data to s3, and making a call to the archive api.
    """
    def __init__(self, path, s3, archive, required_headers=None, blacklist_headers=None):
        self.path = path
        self.s3 = s3
        self.archive = archive
        self.required_headers = required_headers if required_headers else []
        self.blacklist_headers = blacklist_headers if blacklist_headers else []

    def ingest(self):
        # TODO: Add transfer acceleration option
        self.basename, self.extension = get_basename_and_extension(self.path)

        # Get the Md5 checksum of this file and check if it already exists in the archive
        md5 = get_md5(self.path)
        if self.archive.version_exists(md5):
            raise NonFatalDoNotRetryError('Version with this md5 already exists')

        # Transform this fits file into a cleaned dictionary
        fits_dict = FitsDict(self.path, self.required_headers, self.blacklist_headers).as_dict()

        # Figure out the storage class to use based on the date of the observation
        storage_class = get_storage_class(fits_dict)

        # Upload the file to s3 and get version information back
        version = self.s3.upload_file(self.path, storage_class)

        # Make sure our md5 matches amazons
        if version['md5'] != md5:
            raise BackoffRetryError('S3 md5 did not match ours')

        # Construct final archive payload and post to archive
        fits_dict['area'] = wcs_corners_from_dict(fits_dict)
        fits_dict['version_set'] = [version]
        fits_dict['basename'] = self.basename
        result = self.archive.post_frame(fits_dict)

        # Add some useful information from the result
        fits_dict['frameid'] = result.get('id')
        fits_dict['filename'] = result.get('filename')
        return fits_dict
