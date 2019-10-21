#!/bin/env python3
import sys
import argparse
import logging
from logging.config import dictConfig

from settings.log_config import logConf
from ingester.ingester import frame_exists, upload_file_and_ingest_to_archive
from ingester.utils.fits import get_fits_from_path
from ingester.exceptions import NonFatalDoNotRetryError

# Set up logging
dictConfig(logConf)
logger = logging.getLogger('ingester')
logger.setLevel(logging.CRITICAL)  # silence logging for command line tool

description = (
    'Upload a file to the LCO archive. This script will output the resulting URL '
    'if the upload is successful. An optional flag --check-only can be used to '
    'check for the existence of a file without uploading it (based on md5).'
)


def main():
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('path', help='Path to file')
    parser.add_argument('--api-root', help='API root')
    parser.add_argument('--auth-token', help='API token')
    parser.add_argument('--bucket', help='S3 bucket name')
    parser.add_argument('--check-only', action='store_true', help='Only check if the frame exists in the archive. \
                                                                   returns a status code of 0 if found, 1 if not \
                                                                   (or an error occured)')
    args = parser.parse_args()

    try:
        fileobj = get_fits_from_path(args.path)
    except Exception as e:
        sys.stdout.write(str(e))
        sys.exit(1)

    if args.check_only:
        try:
            exists = frame_exists(fileobj, api_root=args.api_root, auth_token=args.auth_token)
        except Exception as e:
            sys.stdout.write(str(e))
            sys.exit(1)
        sys.stdout.write(str(exists))
        sys.exit(int(not exists))

    try:
        result = upload_file_and_ingest_to_archive(fileobj=fileobj, path=fileobj.name, **vars(args))
    except NonFatalDoNotRetryError as e:
        sys.stdout.write(str(e))
        sys.exit(0)
    except Exception as e:
        sys.stdout.write('Exception uploading file: ')
        sys.stdout.write(str(e))
        sys.exit(1)

    sys.stdout.write(result['url'])
    sys.exit(0)


if __name__ == '__main__':
    main()
