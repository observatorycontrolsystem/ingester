import os
import logging
from logging.config import dictConfig
from lcogt_logging import LCOGTFormatter


# logging
logConf = {
    "formatters": {
        "default": {
            "()": LCOGTFormatter
        }
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "default"
        }
    },
    "loggers": {
        "ingester": {
            "handlers": ["console"],
            "level": logging.INFO
        }
    },
    "version": 1
}

dictConfig(logConf)

#  General settings
QUEUE_NAME = os.getenv('QUEUE_NAME', 'ingest_queue')
BROKER_URL = os.getenv('BROKER_URL', 'memory://localhost')
API_ROOT = os.getenv('API_ROOT', 'http://localhost:8000/frames/')
AUTH_TOKEN = os.getenv('API_AUTH_TOKEN', '')

# Fits headers we don't want to ingest
HEADER_BLACKLIST = ('HISTORY', 'COMMENT', '')

# Fits headers that must be present
REQUIRED_HEADERS = ('USERID', 'PROPID', 'DATE-OBS', 'INSTRUME')

#  AWS Credentials and defaults
BUCKET = os.getenv('BUCKET', 'lcogtarchivetest')

# Celery Settings
CELERY_TASK_SERIALIZER = 'json'
CELERY_ACCEPT_CONTENT = ['json']
CELERY_TIMEZONE = 'UTC'
CELERY_ENABLE_UTC = True
CELERYBEAT_SCHEDULE = {}
WORKER_HIJACK_ROOT_LOGGER = False
