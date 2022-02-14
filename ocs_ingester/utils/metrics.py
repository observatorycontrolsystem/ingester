import functools
from datetime import datetime

from opentsdb_python_metrics.metric_wrappers import metric_timer_with_tags, send_tsdb_metric

from ocs_ingester.settings import settings as ingester_settings


def method_timer(metric_name):
    """Decorator to add extra tags to collected runtime metrics"""
    def method_timer_decorator(method):
        def wrapper(self, *args, **kwargs):
            # Decorate the wrapped method with metric_timer_with_tags, which does the work of figuring out
            # how long a method takes to run, so that the settings used are evaluated at runtime. An example
            # of when settings are changed at runtime is when the ingester command line entrypoint is used.
            @metric_timer_with_tags(
                metric_name=metric_name,
                asynchronous=ingester_settings.SUBMIT_METRICS_ASYNCHRONOUSLY,
                **ingester_settings.EXTRA_METRICS_TAGS
            )
            @functools.wraps(method)
            def run_method(self, *args, **kwargs):
                return method(self, *args, **kwargs)
            return run_method(self, *args, **kwargs)
        return wrapper
    return method_timer_decorator

@method_timer('ingester.upload_file')
def upload_and_collect_metrics(filestore_class, datafile):
    start_time = datetime.utcnow()
    # Upload the file to s3 and get version information back
    version = filestore_class.store_file(data_file=datafile)
    # Record metric for the bytes transferred / time to upload
    upload_time = datetime.utcnow() - start_time
    bytes_per_second = len(datafile.open_file) / upload_time.total_seconds()
    send_tsdb_metric(
        metric_name='ingester.s3_upload_bytes_per_second',
        value=bytes_per_second,
        asynchronous=ingester_settings.SUBMIT_METRICS_ASYNCHRONOUSLY,
        **ingester_settings.EXTRA_METRICS_TAGS
    )
    return version

@method_timer('ingester.get_md5')
def get_md5_and_collect_metrics(file_class):
    return file_class.get_md5()
