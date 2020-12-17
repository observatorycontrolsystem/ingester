import functools

from opentsdb_python_metrics.metric_wrappers import metric_timer_with_tags

from ocs_ingester.settings import settings


def method_timer(metric_name):
    """Decorator to add extra tags to collected runtime metrics"""
    def method_timer_decorator(method):
        def wrapper(self, *args, **kwargs):
            # Decorate the wrapped method with metric_timer_with_tags, which does the work of figuring out
            # how long a method takes to run, so that the settings used are evaluated at runtime. An example
            # of when settings are changed at runtime is when the ingester command line entrypoint is used.
            @metric_timer_with_tags(
                metric_name=metric_name,
                asynchronous=settings.SUBMIT_METRICS_ASYNCHRONOUSLY,
                **settings.EXTRA_METRICS_TAGS
            )
            @functools.wraps(method)
            def run_method(self, *args, **kwargs):
                return method(self, *args, **kwargs)
            return run_method(self, *args, **kwargs)
        return wrapper
    return method_timer_decorator
