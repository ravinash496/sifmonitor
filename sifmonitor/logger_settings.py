"""Logging Settings for different loggers"""
import logging
import sys
from logging.config import dictConfig

logging_config = dict(
    version=1,
    formatters={
        'verbose': {
            'format': ("[%(asctime)s] %(levelname)s "
                       "[%(name)s:%(lineno)s] %(message)s"),
            'datefmt': "%d/%b/%Y %H:%M:%S",
        },
        'simple': {
            'format': '%(levelname)s %(message)s',
        },
    },
    handlers={
        'provisioning-service': {'class': 'logging.handlers.RotatingFileHandler',
                                 'formatter': 'verbose',
                                 'level': logging.DEBUG,
                                 'filename': 'logs/provisioning-service.log',
                                 'maxBytes': 52428800,
                                 'backupCount': 7},
        'console': {
            'class': 'logging.StreamHandler',
            'level': 'DEBUG',
            'formatter': 'simple',
            'stream': sys.stdout,
        },
    },
    loggers={
        'provisioning-service_logger': {
            'handlers': ['provisioning-service', 'console'],
            'level': logging.DEBUG
        }
    }
)

dictConfig(logging_config)
# Logger names
logger = logging.getLogger('provisioning-service_logger')
