import logging
from lcogt_logging import LCOGTFormatter


# Logging configuration, can import and setup to use.
logConf = {
    "formatters": {
        "default": {
            "()": LCOGTFormatter
        }
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "default"
        }
    },
    "loggers": {
        "ocs_ingester": {
            "handlers": ["console"],
            "level": logging.INFO,
            "propagate": False,
        }
    },
    "version": 1
}
