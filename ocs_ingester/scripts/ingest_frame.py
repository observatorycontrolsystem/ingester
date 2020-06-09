#!/bin/env python3
"""
Command-line entrypoint to the ingester library.

Examples:

    See available options::

        (venv) ocs_ingest_frame --help

"""
import sys
import argparse

from ocs_ingester.ingester import frame_exists, upload_file_and_ingest_to_archive
from ocs_ingester.settings import settings
from ocs_ingester.exceptions import NonFatalDoNotRetryError

description = (
    'Upload a file to the science archive of an observatory control system. This script will output the resulting URL '
    'if the upload is successful. An optional flag --check-only can be used to check for the existence of a file '
    'without uploading it (based on md5).'
)


def main():
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('path', help='Path to file')
    parser.add_argument('--api-root', help='API root')
    parser.add_argument('--auth-token', help='API token')
    parser.add_argument('--bucket', help='S3 bucket name')
    parser.add_argument('--process-name', help='Tag set in collected metrics')
    parser.add_argument('--check-only', action='store_true', help='Only check if the frame exists in the archive. \
                                                                   returns a status code of 0 if found, 1 if not \
                                                                   (or an error occurred)')
    args = parser.parse_args()

    # Submit metrics synchronously so that they all get submitted before the program exits
    settings.SUBMIT_METRICS_ASYNCHRONOUSLY = False

    if args.process_name:
        settings.EXTRA_METRICS_TAGS['ingester_process_name'] = args.process_name

    try:
        with open(args.path, 'rb') as fileobj:
            if args.check_only:
                try:
                    check_args = {
                        k: v for k, v in vars(args).items() if k in ['api_root', 'auth_token'] and v is not None
                    }
                    exists = frame_exists(fileobj, **check_args)

                except Exception as e:
                    sys.stdout.write(str(e))
                    sys.exit(1)
                sys.stdout.write(str(exists))
                sys.exit(int(not exists))

            try:
                ingest_args = {
                    k: v for k, v in vars(args).items() if k in ['api_root', 'auth_token', 'bucket'] and v is not None
                }
                result = upload_file_and_ingest_to_archive(fileobj=fileobj, path=args.path, **ingest_args)
            except NonFatalDoNotRetryError as e:
                sys.stdout.write(str(e))
                sys.exit(0)
            except Exception as e:
                sys.stdout.write('Exception uploading file: ')
                sys.stdout.write(str(e))
                sys.exit(1)

            sys.stdout.write(result['url'])
            sys.exit(0)

    except Exception as e:
        sys.stdout.write(str(e))
        sys.exit(1)


if __name__ == '__main__':
    main()
