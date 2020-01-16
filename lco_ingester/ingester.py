from lco_ingester.fits import FitsDict
from lco_ingester.exceptions import BackoffRetryError, NonFatalDoNotRetryError
from lco_ingester.utils.fits import wcs_corners_from_dict
from lco_ingester.utils.fits import File
from lco_ingester.archive import ArchiveService
from lco_ingester.s3 import S3Service
from lco_ingester.settings import settings


def frame_exists(fileobj, api_root=settings.API_ROOT, auth_token=settings.AUTH_TOKEN, broker_url=settings.FITS_BROKER):
    """
    Checks if the frame exists in the archive.

    :param fileobj: File-like object
    :param api_root: Archive API root url
    :param auth_token: Archive API authentication token
    :return: Boolean indicating whether the frame exists
    """
    archive = ArchiveService(api_root=api_root, auth_token=auth_token, broker_url=broker_url)
    md5 = File(fileobj, run_validate=False).get_md5()
    return archive.version_exists(md5)


def validate_fits_and_create_archive_record(fileobj, path=None, required_headers=settings.REQUIRED_HEADERS,
                                            blacklist_headers=settings.HEADER_BLACKLIST):
    """
    Validate the fits file and also create an archive record from it.

    :param fileobj: File-like object
    :param path: File path/name for this object
    :param required_headers: FITS headers that must be present
    :param blacklist_headers: FITS headers that should not be ingested
    :return: Constructed archive record
    """
    file = File(fileobj, path)
    json_record = FitsDict(file, required_headers, blacklist_headers).as_dict()
    json_record['area'] = wcs_corners_from_dict(json_record)
    json_record['basename'] = file.basename
    return json_record


def upload_file_to_s3(fileobj, path=None, bucket=settings.BUCKET):
    """
    Uploads a file to s3.

    :param fileobj: File-like object
    :param path: File path/name for this object
    :param bucket: S3 bucket name
    :return: Version information for the file that was uploaded
    """
    file = File(fileobj, path)
    s3 = S3Service(bucket)

    # Transform this fits file into a cleaned dictionary
    fits_dict = FitsDict(file, settings.REQUIRED_HEADERS, settings.HEADER_BLACKLIST).as_dict()

    # Returns the version, which holds in it the md5 that was uploaded
    return s3.upload_file(file, fits_dict)


def ingest_archive_record(version, record, api_root=settings.API_ROOT, auth_token=settings.AUTH_TOKEN,
                          broker_url=settings.FITS_BROKER):
    """
    Ingest an archive record.

    :param version: Result of the upload to s3
    :param record: Archive record to ingest
    :param api_root: Archive API root url
    :param auth_token: Archive API authentication token
    :param broker_url: FITS exchange broker
    :return: The archive record that was ingested
    """
    archive = ArchiveService(api_root=api_root, auth_token=auth_token, broker_url=broker_url)
    # Construct final archive payload and post to archive
    record['version_set'] = [version]
    return archive.post_frame(record)


def upload_file_and_ingest_to_archive(fileobj, path=None, required_headers=settings.REQUIRED_HEADERS,
                                      blacklist_headers=settings.HEADER_BLACKLIST,
                                      api_root=settings.API_ROOT, auth_token=settings.AUTH_TOKEN,
                                      bucket=settings.BUCKET, broker_url=settings.FITS_BROKER):
    """
    Ingest and upload a file.

    :param fileobj: File-like object
    :param path: File path/name for this object
    :param api_root: Archive API root url
    :param auth_token: Archive API authentication token
    :param bucket: S3 bucket name
    :param required_headers: FITS headers that must be present
    :param blacklist_headers: FITS headers that should not be ingested
    :param broker_url: FITS exchange broker
    :return: Information about the uploaded file and record
    """
    file = File(fileobj, path)
    archive = ArchiveService(api_root=api_root, auth_token=auth_token, broker_url=broker_url)
    s3 = S3Service(bucket)
    ingester = Ingester(file, s3, archive, required_headers, blacklist_headers)
    return ingester.ingest()


class Ingester(object):
    """
    Ingest a single file into the archive.

    A single instance of this class is responsible for parsing a fits file,
    uploading the data to s3, and making a call to the archive api.
    """
    def __init__(self, file, s3, archive, required_headers=None, blacklist_headers=None):
        self.file = file
        self.s3 = s3
        self.archive = archive
        self.required_headers = required_headers if required_headers else []
        self.blacklist_headers = blacklist_headers if blacklist_headers else []

    def ingest(self):
        # Get the Md5 checksum of this file and check if it already exists in the archive
        md5 = self.file.get_md5()
        if self.archive.version_exists(md5):
            raise NonFatalDoNotRetryError('Version with this md5 already exists')

        # Transform this fits file into a cleaned dictionary
        fits_dict = FitsDict(self.file, self.required_headers, self.blacklist_headers).as_dict()

        # Upload the file to s3 and get version information back
        version = self.s3.upload_file(self.file, fits_dict)

        # Make sure our md5 matches amazons
        if version['md5'] != md5:
            raise BackoffRetryError('S3 md5 did not match ours')

        # Construct final archive payload and post to archive
        fits_dict['area'] = wcs_corners_from_dict(fits_dict)
        fits_dict['version_set'] = [version]
        fits_dict['basename'] = self.file.basename
        return self.archive.post_frame(fits_dict)
