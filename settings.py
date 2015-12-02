import os

#  General settings

HEADER_BLACKLIST = ['HISTORY', '']

#  AWS Credentials and defaults
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID', 'AKIAIZBYNSZZGYN3EAQQ')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY', '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')
REGION_NAME = os.getenv('REGION_NAME', 'us-west-2')
BUCKET = os.getenv('BUCKET', 'lcogtarchivetest')
