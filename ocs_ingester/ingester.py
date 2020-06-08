"""``ingester.py`` - Top level functions for adding data to the science archive.

Data is added to the science archive using the archive API and an S3 client. The steps necessary to add
data to the science archive are as follows:

    1) Check that the file does not yet exist in the science archive
    2) Validate and build a cleaned dictionary of the headers of the FITS file
    3) Upload the file to S3
    4) Combine the results from steps 2 and 3 into a record to be added to the science archive database

Examples:
    Ingest a file one step at a time:

    >>> from ocs_ingester import ingester
    >>> with open('tst1mXXX-ab12-20191013-0001-e00.fits.fz', 'rb') as fileobj:
    >>>     if not frame_exists(fileobj):
    >>>        record = ingester.validate_fits_and_create_archive_record(fileobj)
    >>>        s3_version = ingester.upload_file_to_s3(fileobj)
    >>>        ingested_record = ingester.ingest_archive_record(s3_version, record)

    Ingest a file in one step:

    >>> from ocs_ingester import ingester
    >>> with open('tst1mXXX-ab12-20191013-0001-e00.fits.fz', 'rb') as fileobj:
    >>>    ingested_record = ingester.upload_file_and_ingest_to_archive(fileobj)

"""

from ocs_ingester.fits import FitsDict
from ocs_ingester.exceptions import BackoffRetryError, NonFatalDoNotRetryError
from ocs_ingester.utils.fits import wcs_corners_from_dict
from ocs_ingester.utils.fits import File
from ocs_ingester.archive import ArchiveService
from ocs_ingester.s3 import S3Service
from ocs_ingester.settings import settings


def frame_exists(fileobj, api_root=settings.API_ROOT, auth_token=settings.AUTH_TOKEN, broker_url=settings.FITS_BROKER):
    """Checks if the file exists in the science archive.

    Computes the md5 of the given file and checks whether a file with that md5 already exists in
    the science archive.

    Args:
        fileobj (file-like object): File-like object
        api_root (str): Science archive API root url
        auth_token (str): Science archive API authentication token
        broker_url (str): FITS exchange broker

    Returns:
        bool: Boolean indicating whether the file exists in the science archive

    Raises:
        ocs_ingester.exceptions.BackoffRetryError: If there was a problem getting
            a response from the science archive API

    """
    archive = ArchiveService(api_root=api_root, auth_token=auth_token, broker_url=broker_url)
    md5 = File(fileobj, run_validate=False).get_md5()
    return archive.version_exists(md5)


def validate_fits_and_create_archive_record(fileobj, path=None, required_headers=settings.REQUIRED_HEADERS,
                                            blacklist_headers=settings.HEADER_BLACKLIST):
    """Validates the FITS file and creates a science archive record from it.

    Checks that required headers are present, removes blacklisted headers, and cleans other
    headers such that they are valid for ingestion into the science archive.

    Args:
        fileobj (file-like object): File-like object
        path (str): File path/name for this object. This option may be used to override the filename
            associated with the fileobj. It must be used if the fileobj does not have a filename.
        required_headers (tuple): FITS headers that must be present
        blacklist_headers (tuple): FITS headers that should not be ingested

    Returns:
        dict: Constructed science archive record. For example::

            {
                'basename': 'tst1mXXX-ab12-20191013-0001-e00',
                'FILTER': 'rp',
                'DATE-OBS': '2019-10-13T10:13:00',
                ...
            }

    Raises:
        ocs_ingester.exceptions.DoNotRetryError: If required headers could not be found

    """
    file = File(fileobj, path)
    json_record = FitsDict(file, required_headers, blacklist_headers).as_dict()
    json_record['area'] = wcs_corners_from_dict(json_record)
    json_record['basename'] = file.basename
    return json_record


def upload_file_to_s3(fileobj, path=None, bucket=settings.BUCKET):
    """Uploads a file to the S3 bucket.

    Args:
        fileobj (file-like object): File-like object
        path (str): File path/name for this object. This option may be used to override the filename
            associated with the fileobj. It must be used if the fileobj does not have a filename.
        bucket (str): S3 bucket name

    Returns:
        dict: Version information for the file that was uploaded. For example::

            {
                'key': '792FE6EFFE6FAD7E',
                'md5': 'ECD9B357D67117BE8BF38D6F4B4A6',
                'extension': '.fits.fz'
            }

    Hint:
        The response contains an "md5" field, which is the md5 computed by S3. It is a good idea to
        check that this md5 is the same as the locally computed md5 of the file to make sure that
        the entire file was successfully uploaded.

    Raises:
        ocs_ingester.exceptions.BackoffRetryError: If there is a problem connecting to S3

    """
    file = File(fileobj, path)
    s3 = S3Service(bucket)

    # Transform this fits file into a cleaned dictionary
    fits_dict = FitsDict(file, settings.REQUIRED_HEADERS, settings.HEADER_BLACKLIST).as_dict()

    # Returns the version, which holds in it the md5 that was uploaded
    return s3.upload_file(file, fits_dict)


def ingest_archive_record(version, record, api_root=settings.API_ROOT, auth_token=settings.AUTH_TOKEN,
                          broker_url=settings.FITS_BROKER):
    """Adds a record to the science archive database.

    Args:
        version (dict): Version information returned from the upload to S3
        record (dict): Science archive record to ingest
        api_root (str): Science archive API root url
        auth_token (str): Science archive API authentication token
        broker_url (str): FITS exchange broker

    Returns:
        dict: The science archive record that was ingested. For example::

        {
            'basename': 'tst1mXXX-ab12-20191013-0001-e00',
            'version_set': [
                {
                    'key': '792FE6EFFE6FAD7E',
                    'md5': 'ECD9B357D67117BE8BF38D6F4B4A6',
                    'extension': '.fits.fz'
                    }
                ],
            'frameid': 12345,
            ...
        }

    Raises:
        ocs_ingester.exceptions.BackoffRetryError: If there was a problem connecting to the science archive
        ocs_ingester.exceptions.DoNotRetryError: If there was a problem with the record that must be fixed before
            attempting to ingest it again

    """
    archive = ArchiveService(api_root=api_root, auth_token=auth_token, broker_url=broker_url)
    # Construct final archive payload and post to archive
    record['version_set'] = [version]
    return archive.post_frame(record)


def upload_file_and_ingest_to_archive(fileobj, path=None, required_headers=settings.REQUIRED_HEADERS,
                                      blacklist_headers=settings.HEADER_BLACKLIST,
                                      api_root=settings.API_ROOT, auth_token=settings.AUTH_TOKEN,
                                      bucket=settings.BUCKET, broker_url=settings.FITS_BROKER):
    """Uploads a file to S3 and adds the associated record to the science archive database.

    This is a standalone function that runs all of the necessary steps to add data to the
    science archive.

    Args:
        fileobj (file-like object): File-like object
        path (str): File path/name for this object. This option may be used to override the filename
            associated with the fileobj. It must be used if the fileobj does not have a filename.
        api_root (str): Science archive API root url
        auth_token (str): Science archive API authentication token
        bucket (str): S3 bucket name
        required_headers (tuple): FITS headers that must be present
        blacklist_headers (tuple): FITS headers that should not be ingested
        broker_url (str): FITS exchange broker

    Returns:
        dict: Information about the uploaded file and record. For example:

            {
                'basename': 'tst1mXXX-ab12-20191013-0001-e00',
                'version_set': [
                    {
                        'key': '792FE6EFFE6FAD7E',
                        'md5': 'ECD9B357D67117BE8BF38D6F4B4A6',
                        'extension': '.fits.fz'
                        }
                    ],
                'frameid': 12345,
                ...
            }

    Raises:
        ocs_ingester.exceptions.NonFatalDoNotRetryError: If the file already exists in the science archive
        ocs_ingester.exceptions.BackoffRetryError: If the md5 computed locally does not match the md5
            computed by S3, if there was an error connecting to S3, or if there was a problem reaching
            the science archive.
        ocs_ingester.exceptions.DoNotRetryError: If there was a problem that must be fixed before attempting
             to ingest again

    """
    file = File(fileobj, path)
    archive = ArchiveService(api_root=api_root, auth_token=auth_token, broker_url=broker_url)
    s3 = S3Service(bucket)
    ingester = Ingester(file, s3, archive, required_headers, blacklist_headers)
    return ingester.ingest()


class Ingester(object):
    """Ingest a single file into the archive.

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
