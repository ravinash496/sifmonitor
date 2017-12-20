import configparser
import json
from logger_settings import *


def read_config_file():
    """
    config parser
    :return: 
    """
    config_file = 'sifmonitor.ini'
    config = configparser.ConfigParser()
    try:
        config.read(config_file)
    except IOError as ie:
        logger.error(ie)
    return config


def read_json(credential_file):
    """
    Read configuration file
    :param credential_file: 
    :return: 
    """
    try:
        with open(credential_file, 'r') as fp:
            return json.load(fp)
    except IOError as error:
        logger.error(error)

config = read_config_file()
application_flag = config["Default"]["application_flag"]
THREAD_SLEEP_TIME = eval(config["Default"]["THREAD_SLEEP_TIME"])
target_schema = config["Default"]["target_schema"]
service_urn_file = "serviceurnlayermapping.py"
SERVER_NAME = config["App"]["SERVER_NAME"]
