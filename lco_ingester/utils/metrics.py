import functools

from opentsdb_python_metrics.metric_wrappers import metric_timer_with_tags

from lco_ingester.settings.settings import EXTRA_METRICS_TAGS


def method_timer(metric_name):
    """Decorator to add extra tags to collected runtime metrics"""
    def method_timer_decorator(method):
        def wrapper(self, *args, **kwargs):
            # Decorate the wrapped method with metric_timer_with_tags, which does the work of figuring out
            # the runtime, so that the EXTRA_METRICS_TAGS are evaluated at runtime. An example of when the
            # value is changed at runtime is when the ingester command line entrypoint is used.
            @metric_timer_with_tags(metric_name, **EXTRA_METRICS_TAGS)
            @functools.wraps(method)
            def run_method(self, *args, **kwargs):
                return method(self, *args, **kwargs)
            return run_method(self, *args, **kwargs)
        return wrapper
    return method_timer_decorator
