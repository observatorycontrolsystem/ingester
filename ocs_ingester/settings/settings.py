import os
import ast


# General settings
API_ROOT = os.getenv('API_ROOT', 'http://127.0.0.1:8000/')
AUTH_TOKEN = os.getenv('AUTH_TOKEN', 'c158b4f055c5abdd9f520c8501159478f6f738ac')

# Whether to submit the metrics asynchronously
SUBMIT_METRICS_ASYNCHRONOUSLY = ast.literal_eval(os.getenv('SUBMIT_METRICS_ASYNCHRONOUSLY', 'False'))

# Extra tags for metrics
EXTRA_METRICS_TAGS = {
    'ingester_process_name': os.getenv('INGESTER_PROCESS_NAME', 'ingester')
}
