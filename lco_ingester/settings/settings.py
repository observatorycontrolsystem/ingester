import os


def get_tuple_from_environment(variable_name, default):
    return tuple(os.getenv(variable_name, default).strip(',').replace(' ', '').split(','))


# General settings
FITS_BROKER = os.getenv('FITS_BROKER', 'memory://localhost')
API_ROOT = os.getenv('API_ROOT', 'http://localhost:8000/')
AUTH_TOKEN = os.getenv('AUTH_TOKEN', '')

# AWS Credentials and defaults
BUCKET = os.getenv('BUCKET', 'ingestertest')

# Files we wish to ignore
IGNORED_CHARS = get_tuple_from_environment('IGNORED_CHARS', '-t00,-x00,-g00,-l00,-kb11,-kb15,tstnrs')

# Fits headers we don't want to ingest
HEADER_BLACKLIST = get_tuple_from_environment('HEADER_BLACKLIST', 'HISTORY,COMMENT')

# Fits headers that must be present
REQUIRED_HEADERS = get_tuple_from_environment('REQUIRED_HEADERS', 'PROPID,DATE-OBS,INSTRUME,SITEID,TELID,OBSTYPE,BLKUID')

# Calibration observation types (OBSTYPE)
CALIBRATION_TYPES = get_tuple_from_environment('CALIBRATION_TYPES', 'BIAS,DARK,SKYFLAT,EXPERIMENTAL')

# Proposals including these strings will be considered public data
PUBLIC_PROPOSALS = get_tuple_from_environment('PUBLIC_PROPOSALS', 'EPO,calib,standard,pointing')

# Crawler RabbitMQ Exchange Name
CRAWLER_EXCHANGE_NAME = os.getenv('CRAWLER_EXCHANGE_NAME', 'fits_files')

# Processed files RabbitMQ Exchange Name
PROCESSED_EXCHANGE_NAME = os.getenv('PROCESSED_EXCHANGE_NAME', 'archived_fits')
