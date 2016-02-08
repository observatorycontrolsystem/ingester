import hashlib
import boto3
"""
This module contains helper functions for uploading files to amazon s3
"""


def get_client(access_key, secret_key, region):
    return boto3.Session(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region
    ).client('s3')


def basename_to_s3_key(basename):
    """
    Place files in subfolders where the folders are the first 4 characters of
    the sha1 checksum of the filename. This is to avoid potential performance
    issues with s3
    """
    return '/'.join((hashlib.sha1(basename.encode('utf-8')).hexdigest()[0:4], basename))


def extension_to_content_type(extension):
    content_types = {
        '.fits': 'image/fits',
        '.tar.gz': 'application/x-tar',
    }
    return content_types.get(extension, '')


def strip_quotes_from_etag(etag):
    """
    Amazon returns the md5 sum of the uploaded
    file in the 'ETag' header wrapped in quotes
    """
    if etag.startswith('"') and etag.endswith('"'):
        return etag[1:-1]


def get_md5(file):
    return hashlib.md5(file.read()).hexdigest()
