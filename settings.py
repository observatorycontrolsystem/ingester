import os
import logging
from logging.config import dictConfig
from lcogt_logging import LCOGTFormatter
from datetime import timedelta


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
API_ROOT = os.getenv('API_ROOT', 'http://localhost:8000/')
AUTH_TOKEN = os.getenv('AUTH_TOKEN', '')

# Fits headers we don't want to ingest
HEADER_BLACKLIST = ('HISTORY', 'COMMENT', '')

# Fits headers that must be present
REQUIRED_HEADERS = ('PROPID', 'DATE-OBS', 'INSTRUME', 'SITEID', 'TELID', 'OBSTYPE')

#  AWS Credentials and defaults
BUCKET = os.getenv('BUCKET', 'lcogtarchivetest')

# Celery Settings
CELERYD_PREFETCH_MULTIPLIER = 1
CELERY_TASK_SERIALIZER = 'json'
CELERY_ACCEPT_CONTENT = ['json']
CELERY_TIMEZONE = 'UTC'
CELERY_ENABLE_UTC = True
WORKER_HIJACK_ROOT_LOGGER = False

CELERYBEAT_SCHEDULE = {
    'queue-length-every-minute': {
        'task': 'tasks.collect_queue_length_metric',
        'schedule': timedelta(minutes=1),
        'args': ('http://cerberus.lco.gtn:15672/',),
        'options': {'queue': 'periodic'}
    },
    'total-holdings-every-5-minutes': {
        'task': 'tasks.total_holdings',
        'schedule': timedelta(minutes=5),
        'args': (API_ROOT, AUTH_TOKEN),
        'options': {'queue': 'periodic'}
    }
}
