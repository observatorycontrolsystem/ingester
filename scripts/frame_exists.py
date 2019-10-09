import argparse
import logging

from ingester.ingester import frame_exists

logger = logging.getLogger('ingester')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('path', help='Path to file')
    parser.add_argument('--api-root', help='API root')
    parser.add_argument('--auth-token', help='API token')
    args = parser.parse_args()

    exists = frame_exists(args.path, api_root=args.api_root, auth_token=args.auth_token)
    logger.info(exists)
