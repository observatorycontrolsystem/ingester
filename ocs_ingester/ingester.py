"""``ingester.py`` - Top level functions for adding data to the science archive.

Data is added to the science archive using the archive API and an S3 client. The steps necessary to add
data to the science archive are as follows:

    1) Check that the file does not yet exist in the science archive
    2) Validate and build a cleaned dictionary of the headers of the FITS file, or from provided metadata when uploading
       a non-FITS file
    3) Upload the file to S3
    4) Combine the results from steps 2 and 3 into a record to be added to the science archive database

Examples:
    Ingest a file one step at a time:

    >>> from ocs_ingester import ingester
    >>> with open('tst1mXXX-ab12-20191013-0001-e00.fits.fz', 'rb') as fileobj:
    >>>     if not ingester.frame_exists(fileobj):
    >>>        record = ingester.validate_fits_and_create_archive_record(fileobj)
    >>>        s3_version = ingester.upload_file_to_s3(fileobj)
    >>>        ingested_record = ingester.ingest_archive_record(s3_version, record)

    Ingest a file in one step:

    >>> from ocs_ingester import ingester
    >>> with open('tst1mXXX-ab12-20191013-0001-e00.fits.fz', 'rb') as fileobj:
    >>>    ingested_record = ingester.upload_file_and_ingest_to_archive(fileobj)

"""

from ocs_ingester.exceptions import BackoffRetryError, NonFatalDoNotRetryError, DoNotRetryError
from ocs_ingester.archive import ArchiveService
from ocs_ingester.utils.metrics import upload_and_collect_metrics, get_md5_and_collect_metrics
from ocs_ingester.settings import settings as ingester_settings

from ocs_archive.settings import settings as archive_settings
from ocs_archive.input.file import File, FileSpecificationException
from ocs_archive.input.filefactory import FileFactory
from ocs_archive.storage.filestorefactory import FileStoreFactory
from ocs_archive.storage.filestore import FileStoreSpecificationError, FileStoreConnectionError


def frame_exists(fileobj, api_root=ingester_settings.API_ROOT, auth_token=ingester_settings.AUTH_TOKEN):
    """Checks if the file exists in the science archive.

    Computes the md5 of the given file and checks whether a file with that md5 already exists in
    the science archive.

    Args:
        fileobj (file-like object): File-like object
        api_root (str): Science archive API root url
        auth_token (str): Science archive API authentication token

    Returns:
        bool: Boolean indicating whether the file exists in the science archive

    Raises:
        ocs_ingester.exceptions.BackoffRetryError: If there was a problem getting
            a response from the science archive API

    """
    archive = ArchiveService(api_root=api_root, auth_token=auth_token)
    md5 = get_md5_and_collect_metrics(File(fileobj))
    return archive.version_exists(md5)


def validate_fits_and_create_archive_record(fileobj, path=None, file_metadata=None,
                                            required_headers=archive_settings.REQUIRED_HEADERS,
                                            blacklist_headers=archive_settings.HEADER_BLACKLIST):
    """Validates the FITS file and creates a science archive record from it.

    Checks that required headers are present, removes blacklisted headers, and cleans other
    headers such that they are valid for ingestion into the science archive.

    Args:
        fileobj (file-like object): File-like object
        path (str): File path/name for this object. This option may be used to override the filename
            associated with the fileobj. It must be used if the fileobj does not have a filename.
        file_metadata (dict): Dictionary of file metadata to use when generating the archive record for a non-FITS file.
            This must be used when uploading a non-FITS file.
        required_headers (tuple): FITS headers that must be present
        blacklist_headers (tuple): FITS headers that should not be ingested

    Returns:
        dict: Constructed science archive record. For example::

            {
                'basename': 'tst1mXXX-ab12-20191013-0001-e00',
                'headers': {
                    'FILTER': 'rp',
                    'DATE-OBS': '2019-10-13T10:13:00',
                    ...
                },
                'area': ...
                ...
            }

    Raises:
        ocs_ingester.exceptions.DoNotRetryError: If required headers could not be found

    """
    try:
        open_file = File(fileobj, path)
        datafile = FileFactory.get_datafile_class_for_extension(open_file.extension)(
            open_file, file_metadata, blacklist_headers, required_headers
        )
    except FileSpecificationException as fe:
        raise DoNotRetryError(str(fe))
    json_record = datafile.get_header_data().get_archive_frame_data()
    json_record['headers'] = datafile.get_header_data().get_headers()
    json_record['area'] = datafile.get_wcs_corners()
    json_record['basename'] = open_file.basename
    return json_record


def upload_file_to_file_store(fileobj, path=None, file_metadata=None):
    """Uploads a file to the S3 bucket.

    Args:
        fileobj (file-like object): File-like object
        path (str): File path/name for this object. This option may be used to override the filename
            associated with the fileobj. It must be used if the fileobj does not have a filename.
        file_metadata (dict): Dictionary of file metadata to use when generating the archive record for a non-FITS file.
            This must be used when uploading a non-FITS file.

    Returns:
        dict: Version information for the file that was uploaded. For example::

            {
                'key': '792FE6EFFE6FAD7E',
                'md5': 'ECD9B357D67117BE8BF38D6F4B4A6',
                'extension': '.fits.fz'
            }

    Hint:
        The response contains an "md5" field, which is the md5 computed by file store. It is a good idea to
        check that this md5 is the same as the locally computed md5 of the file to make sure that
        the entire file was successfully uploaded.

    Raises:
        ocs_ingester.exceptions.BackoffRetryError: If there is a problem connecting to file store
        ocs_ingester.exceptions.DoNotRetryError: If there is a problem configuring file store
    """
    try:
        open_file = File(fileobj, path)
        datafile = FileFactory.get_datafile_class_for_extension(open_file.extension)(
            open_file, file_metadata
        )
    except FileSpecificationException as fe:
        raise DoNotRetryError(str(fe))

    try:
        filestore = FileStoreFactory.get_file_store_class()()
        # Returns the version, which holds in it the md5 that was uploaded
        return upload_and_collect_metrics(filestore, datafile)
    except FileStoreSpecificationError as fe:
        raise DoNotRetryError(str(fe))
    except FileStoreConnectionError as fce:
        raise BackoffRetryError(str(fce))


def ingest_archive_record(version, record, api_root=ingester_settings.API_ROOT,
                          auth_token=ingester_settings.AUTH_TOKEN):
    """Adds a record to the science archive database.

    Args:
        version (dict): Version information returned from the upload to S3
        record (dict): Science archive record to ingest
        api_root (str): Science archive API root url
        auth_token (str): Science archive API authentication token

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
            'headers': {
                'REQNUM': 12345,
                ...
            }
            ...
        }

    Raises:
        ocs_ingester.exceptions.BackoffRetryError: If there was a problem connecting to the science archive
        ocs_ingester.exceptions.DoNotRetryError: If there was a problem with the record that must be fixed before
            attempting to ingest it again

    """
    archive = ArchiveService(api_root=api_root, auth_token=auth_token)
    # Construct final archive payload and post to archive
    record['version_set'] = [version]
    return archive.post_frame(record)


def upload_file_and_ingest_to_archive(fileobj, path=None, file_metadata=None,
                                      required_headers=archive_settings.REQUIRED_HEADERS,
                                      blacklist_headers=archive_settings.HEADER_BLACKLIST,
                                      api_root=ingester_settings.API_ROOT, auth_token=ingester_settings.AUTH_TOKEN):
    """Uploads a file to S3 and adds the associated record to the science archive database.

    This is a standalone function that runs all of the necessary steps to add data to the
    science archive.

    Args:
        fileobj (file-like object): File-like object
        path (str): File path/name for this object. This option may be used to override the filename
            associated with the fileobj. It must be used if the fileobj does not have a filename.
        file_metadata (dict): Dictionary of file metadata to use when generating the archive record for a non-FITS file.
            This must be used when uploading a non-FITS file.
        api_root (str): Science archive API root url
        auth_token (str): Science archive API authentication token
        required_headers (tuple): FITS headers that must be present
        blacklist_headers (tuple): FITS headers that should not be ingested

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
                'headers': {
                    'REQNUM': 12345,
                    ...
                }
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
    try:
        if file_metadata is None:
            file_metadata = {}
        open_file = File(fileobj, path)
        datafile = FileFactory.get_datafile_class_for_extension(open_file.extension)(
            open_file, file_metadata, required_headers=required_headers, blacklist_headers=blacklist_headers
        )
        filestore = FileStoreFactory.get_file_store_class()()
    except (FileSpecificationException, FileStoreSpecificationError) as fe:
        raise DoNotRetryError(str(fe))

    archive = ArchiveService(api_root=api_root, auth_token=auth_token)
    ingester = Ingester(datafile, filestore, archive)
    return ingester.ingest()


class Ingester(object):
    """Ingest a single file into the archive.

    A single instance of this class is responsible for parsing a fits file,
    uploading the data to s3, and making a call to the archive api.
    """
    def __init__(self, datafile, filestore, archive):
        self.datafile = datafile
        self.filestore = filestore
        self.archive = archive

    def ingest(self):
        # Get the Md5 checksum of this file and check if it already exists in the archive
        md5 = get_md5_and_collect_metrics(self.datafile.open_file)
        if self.archive.version_exists(md5):
            raise NonFatalDoNotRetryError('Version with this md5 already exists')

        # Upload the file to s3 and get version information back
        version = upload_and_collect_metrics(self.filestore, self.datafile)

        # Make sure our md5 matches amazons
        if version['md5'] != md5:
            raise BackoffRetryError('S3 md5 did not match ours')

        # Construct final archive payload and post to archive
        record = self.datafile.get_header_data().get_archive_frame_data()
        record['headers'] = self.datafile.get_header_data().get_headers()
        record['area'] = self.datafile.get_wcs_corners()
        record['version_set'] = [version]
        record['basename'] = self.datafile.open_file.basename
        return self.archive.post_frame(record)
