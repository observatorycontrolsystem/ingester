#!/bin/env python3
import argparse
import logging
import sys

from ingester.ingester import frame_exists, upload_file_and_ingest_to_archive
from ingester.exceptions import NonFatalDoNotRetryError

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
                                                                   returns a status code of 0 if found, 1 if not.')
    args = parser.parse_args()

    if args.check_only:
        exists = frame_exists(args.path, api_root=args.api_root, auth_token=args.auth_token)
        sys.stdout.write(str(exists))
        sys.exit(int(not exists))

    try:
        result = upload_file_and_ingest_to_archive(**vars(args))
    except NonFatalDoNotRetryError:
        sys.stdout.write('File already exists in archive.')
        sys.exit(1)
    except Exception as e:
        sys.stdout.write('Exception uploading file: ')
        sys.stdout.write(str(e))
        sys.exit(1)

    sys.stdout.write(result['url'])
    sys.exit(0)


if __name__ == '__main__':
    main()
