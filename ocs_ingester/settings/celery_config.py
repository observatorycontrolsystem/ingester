import os
from datetime import timedelta

from ocs_ingester.settings import settings


# Celery Broker
broker_url = os.getenv('BROKER_URL', 'memory://localhost')

# Celery Settings
celery_config = {
    'broker_url': broker_url,
    'task_soft_time_limit': 1200,
    'task_time_limit': 3600,
    'worker_prefetch_multiplier': 1,
    'worker_max_tasks_per_child': 10,
    'beat_schedule': {
        'queue-length-every-minute': {
            'task': 'tasks.collect_queue_length_metric',
            'schedule': timedelta(minutes=1),
            'args': ('http://ingesterrabbitmq:15672/',),
            'options': {'queue': 'periodic'}
        },
        'total-holdings-every-5-minutes': {
            'task': 'tasks.total_holdings',
            'schedule': timedelta(minutes=5),
            'args': (settings.API_ROOT, settings.AUTH_TOKEN),
            'options': {'queue': 'periodic'}
        }
    }
}
