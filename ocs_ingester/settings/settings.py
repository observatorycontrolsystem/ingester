import os
import ast


def get_tuple_from_environment(variable_name, default):
    return tuple(os.getenv(variable_name, default).strip(',').replace(' ', '').split(','))


# General settings
FITS_BROKER = os.getenv('FITS_BROKER', 'memory://localhost')
API_ROOT = os.getenv('API_ROOT', 'http://localhost:8000/')
AUTH_TOKEN = os.getenv('AUTH_TOKEN', '')

# AWS Credentials and defaults
BUCKET = os.getenv('BUCKET', 'ingestertest')

# Files we wish to ignore
IGNORED_CHARS = get_tuple_from_environment('IGNORED_CHARS', '-l00,tstnrs')

# Fits headers we don't want to ingest
HEADER_BLACKLIST = get_tuple_from_environment('HEADER_BLACKLIST', 'HISTORY,COMMENT')

# Fits headers that must be present
REQUIRED_HEADERS = get_tuple_from_environment('REQUIRED_HEADERS', 'PROPID,DATE-OBS,INSTRUME,SITEID,TELID,OBSTYPE,BLKUID')

# Calibration observation types (OBSTYPE)
CALIBRATION_TYPES = get_tuple_from_environment('CALIBRATION_TYPES', 'BIAS,DARK,SKYFLAT,EXPERIMENTAL')

# Proposals including these strings will be considered public data
PUBLIC_PROPOSALS = get_tuple_from_environment('PUBLIC_PROPOSALS', 'EPO,calib,standard,pointing')

# Proposals including these strings will be considered private data (L1PUBDATE far out)
PRIVATE_PROPOSALS = get_tuple_from_environment('PRIVATE_PROPOSALS', 'LCOEngineering')

# File types which are private (L1PUBDATE far out)
PRIVATE_FILE_TYPES = get_tuple_from_environment('PRIVATE_FILE_TYPES', '-t00,-x00')

# Crawler RabbitMQ Exchange Name
CRAWLER_EXCHANGE_NAME = os.getenv('CRAWLER_EXCHANGE_NAME', 'fits_files')

# Processed files RabbitMQ Exchange Name
PROCESSED_EXCHANGE_NAME = os.getenv('PROCESSED_EXCHANGE_NAME', 'archived_fits')

# Optionally submit files to fits queue
POSTPROCESS_FILES = ast.literal_eval(os.getenv('POSTPROCESS_FILES', 'True'))

# Whether to submit the metrics asynchronously
SUBMIT_METRICS_ASYNCHRONOUSLY = ast.literal_eval(os.getenv('SUBMIT_METRICS_ASYNCHRONOUSLY', 'False'))

# Extra tags for metrics
EXTRA_METRICS_TAGS = {
    'ingester_process_name': os.getenv('INGESTER_PROCESS_NAME', 'ingester')
}
