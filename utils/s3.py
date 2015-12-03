import hashlib
import boto3


def get_client(access_key, secret_key, region):
    return boto3.Session(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region
    ).client('s3')


def filename_to_s3_key(filename):
    return '/'.join((hashlib.sha1(filename.encode('utf-8')).hexdigest()[0:4], filename))
