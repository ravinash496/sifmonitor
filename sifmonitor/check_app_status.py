import os
import requests
import subprocess
import settings
import time
from logger_settings import *

SERVER_HOST1 =os.environ.get('SERVER_HOST1')
# SERVER_HOST1 = 'host ip address'


def ping_server():
    urls = get_urls()
    server_status = []
    for url in urls:
        try:
            res = requests.get(url.get('url'))
            logger.info("SIFMONITOR Server: {} is UP and running".format(url.get('host')))
            server_status.append(True)
        except requests.exceptions.ConnectionError as error:
            logger.error(error)
            server_status.append(False)
            logger.info("SIFMONITOR Server: {} is DOWN and a new master has to be selected".format(url.get('host')))
            continue
    return server_status


def get_urls():
    sif_monitor_details = settings.SIFMONITOR_CREDENTIALS[settings.SERVER_NAME]
    if settings.SERVER_NAME == 'SIF_MONITOR_BACKUP1':
        url = [{"url": "http://{}:{}/ping".format(SERVER_HOST1, sif_monitor_details['MASTER_SERVER_PORT']), "host": "MASTER"}]

    elif settings.SERVER_NAME == 'SIF_MONITOR_BACKUP2':
        url1 = "http://{}:{}/ping".format(SERVER_HOST1, sif_monitor_details['MASTER_SERVER_PORT'])
        url2 = "http://{}:{}/ping".format(SERVER_HOST1, sif_monitor_details['BACKUP_SERVER_PORT'])
        url = [{'url': url1, 'host': 'MASTER'}, {'url': url2, 'host': 'BACKUP1'}]

    return url


def start_provision_aggregator():
    logger.info("Starting provision aggregator!!!")
    # Python_bin is path of your virtual environment
    python_bin = r""
    # script_file is provision_aggregator path
    script_file = r""
    # subprocess.call([python_bin, script_file])
    subprocess.call([python_bin, script_file])


def start_app():
    logger.info("Starting app.py!!!")
    # Python_bin is path of your virtual environment
    python_bin = r""
    # script_file is app.py path
    script_file = r""
    #  subprocess.Popen([python_bin, script_file1])
    subprocess.Popen([python_bin, script_file])


while 1:

    if settings.SERVER_NAME == 'SIF_MONITOR_MASTER':
        start_app()
        start_provision_aggregator()
    else:
        start_app()
    response = ping_server()
    if not any(response):
        start_provision_aggregator()
        break
    time.sleep(5)
