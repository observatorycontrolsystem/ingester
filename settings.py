import os

#  General settings
QUEUE_NAME = os.getenv('QUEUE_NAME', 'ingest_queue')
BROKER_URL = os.getenv('BROKER_URL', 'memory://localhost')
API_ROOT = os.getenv('API_ROOT', 'http://localhost:8000')

# Fits headers we don't want to ingest
HEADER_BLACKLIST = ['HISTORY', '']

#  AWS Credentials and defaults
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID', 'AKIAIZBYNSZZGYN3EAQQ')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY', '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')
REGION_NAME = os.getenv('REGION_NAME', 'us-west-2')
BUCKET = os.getenv('BUCKET', 'lcogtarchivetest')

# Celery Settings
CELERY_RESULT_BACKEND = BROKER_URL
CELERY_TASK_SERIALIZER = 'json'
CELERY_ACCEPT_CONTENT = ['json']
CELERY_TIMEZONE = 'UTC'
CELERY_ENABLE_UTC = True
CELERYBEAT_SCHEDULE = {}
