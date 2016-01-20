import hashlib
import os
import boto3


def get_client(access_key, secret_key, region):
    return boto3.Session(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region
    ).client('s3')


def filename_to_s3_key(filename):
    filename = os.path.basename(filename)
    return '/'.join((hashlib.sha1(filename.encode('utf-8')).hexdigest()[0:4], filename))


def filename_to_content_type(filename):
    content_types = {
        '.fits': 'image/fits',
        '.tar.gz': 'application/x-tar',
    }
    for t in content_types:
        if filename.endswith(t):
            return content_types[t]
        return ''


def strip_quotes_from_etag(etag):
    """
    Amazon returns the md5 sum of the uploaded
    file in the 'ETag' header wrapped in quotes
    """
    if etag.startswith('"') and etag.endswith('"'):
        return etag[1:-1]
