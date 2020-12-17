"""
Exceptions raised by the ingester library.
"""


class DoNotRetryError(Exception):
    """
    Exception that is raised when an error happens that
    will undoubtedly repeat if called again. The task should
    not be retried.
    """
    pass


class RetryError(Exception):
    """
    Exception that is raised when an error happens that
    can be retried.
    """
    pass


class BackoffRetryError(Exception):
    """
    Exception that is raised when an error happens that
    can be retried with an expontential backoff. For example,
    networking latency errors that may succeed at a later time.
    """
    pass


class NonFatalDoNotRetryError(Exception):
    """
    Exception that is raised when an error happens that
    should not be retried and is also not a fatal condition.
    """
    pass
