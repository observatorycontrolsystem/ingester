class DoNotRetryError(Exception):
    pass


class RetryError(Exception):
    pass


class BackoffRetryError(Exception):
    pass
