import os

# General settings
FITS_BROKER = os.getenv('FITS_BROKER', 'memory://localhost')
API_ROOT = os.getenv('API_ROOT', 'http://localhost:8000/')
AUTH_TOKEN = os.getenv('AUTH_TOKEN', '')

# AWS Credentials and defaults
BUCKET = os.getenv('BUCKET', 'ingestertest')

# Files we wish to ignore
IGNORED_CHARS = tuple(os.getenv('IGNORED_CHARS', "-t00,-x00,-g00,-l00,-kb11,-kb15,tstnrs").strip(',').replace(' ','').split(','))

# Fits headers we don't want to ingest
HEADER_BLACKLIST = tuple(os.getenv('HEADER_BLACKLIST', "HISTORY,COMMENT").strip(',').replace(' ','').split(','))

# Fits headers that must be present
REQUIRED_HEADERS = tuple(os.getenv('REQUIRED_HEADERS', "PROPID,DATE-OBS,INSTRUME,SITEID,TELID,OBSTYPE,BLKUID").strip(',').replace(' ','').split(','))

# Calibration observation types (OBSTYPE)
CALIBRATION_TYPES = tuple(os.getenv('CALIBRATION_TYPES', "BIAS,DARK,SKYFLAT,EXPERIMENTAL").strip(',').replace(' ','').split(','))

# Proposals including these strings will be considered public data
PUBLIC_PROPOSALS = tuple(os.getenv('PUBLIC_PROPOSALS', "EPO,calib,standard,pointing").strip(',').replace(' ','').split(','))

# Crawler RabbitMQ Exchange Name
CRAWLER_EXCHANGE_NAME = os.getenv('CRAWLER_EXCHANGE_NAME', 'fits_files')

# Processed files RabbitMQ Exchange Name
PROCESSED_EXCHANGE_NAME = os.getenv('PROCESSED_EXCHANGE_NAME', 'archived_fits')
