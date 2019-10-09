class DoNotRetryError(Exception):
    """
    Raise an exception and do not attempt to retry the
    containing task. This should be raised when an
    error occurs that will undoubtedly occur if called
    again.
    """
    pass


class RetryError(Exception):
    """
    Raise an exception, but allow for the wrapping task
    to retry. The task will be retried after the
    default_retry_delay a maximum of max_retries times.
    """
    pass


class BackoffRetryError(Exception):
    """
    Raise an exception, but allow for the wrapping task
    to retry. The task will be retried in an exponential
    backoff a maximum of max_retries times. This is useful
    for networking latency errors that may succeeed at
    a later time.
    """
    pass


class NonFatalDoNotRetryError(Exception):
    """
    An exception has been raised, but everything
    should be OK anyway
    """
    pass
