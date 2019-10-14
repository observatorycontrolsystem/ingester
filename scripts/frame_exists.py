#!/bin/env python3
import argparse
import logging

from ingester.ingester import frame_exists, upload_file_and_ingest_to_archive

logger = logging.getLogger('ingester')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('path', help='Path to file')
    parser.add_argument('--api-root', help='API root')
    parser.add_argument('--auth-token', help='API token')
    parser.add_argument('--bucket', help='S3 bucket name')
    parser.add_argument('--check-only', action='store_true', help='Only check if the frame exists in the archive. \
                                                                   returns a status code of 0 if found, 1 if not.')
    args = parser.parse_args()

    if args.check_only:
        exists = frame_exists(args.path, api_root=args.api_root, auth_token=args.auth_token)
        logger.info(exists)
        return int(not exists)

    return upload_file_and_ingest_to_archive(**vars(args))


if __name__ == '__main__':
    main()
