import json
import os
from logger_settings import *


CREDENTIAL_FILE = 'connection/connection.json'
application_flag = 'application_flag.txt'
THREAD_SLEEP_TIME = 30
target_schema = "provisioning"
service_urn_file = "serviceurnlayermapping.py"
SERVER_NAME = "SIF_MONITOR_{}".format(os.environ.get('SIFCONFIGFILE'))
#SERVER_NAME = "SIF_MONITOR_BACKUP1"


SIFMONITOR_CREDENTIALS = {
    "SIF_MONITOR_MASTER": {
        "SERVER_HOST": '0.0.0.0',
        "APP_SERVER_PORT": 9000,
        "MASTER_SERVER_PORT": '9000'},

    "SIF_MONITOR_BACKUP1": {
        "SERVER_HOST": '0.0.0.0',
        "APP_SERVER_PORT": 9001,
        "MASTER_SERVER_PORT": '9000'},

    "SIF_MONITOR_BACKUP2": {
        "SERVER_HOST": '0.0.0.0',
        "APP_SERVER_PORT": 9002,
        "MASTER_SERVER_PORT": '9000',
        "BACKUP_SERVER_PORT": '9001'}
    }


def read_json(credential_file):
    """Read JSON setting file"""
    try:
        with open(credential_file, 'r') as fp:
            return json.load(fp)
    except IOError as error:
        logger.error(error)
