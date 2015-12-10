class DoNotRetryError(Exception):
    """
    Raise an exception and do not attempt to retry the
    containing task. This should be raised when an
    error occurs that will undoubtedly occur if called
    again
    """
    pass


class RetryError(Exception):
    """
    Raise an exception, but allow for the wrapping task
    to retry. The task will be retried according to the
    default_retry_delay a maximum of max_retries times
    task arguments.
    """
    pass


class BackoffRetryError(Exception):
    """
    Raise an exception, but allow for the wrapping task
    to retry. The task will be retried in an exponential
    backoff a maximum of max_retries times according
    to the task argument. This useful for networking
    latency errors that may succeeed at a later time.
    """
    pass
