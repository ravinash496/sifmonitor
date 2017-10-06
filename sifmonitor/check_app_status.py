import requests
import subprocess
import settings
import time
from logger_settings import *


def ping_server():
    urls = get_urls()
    server_status = []
    for url in urls:
        try:
            res = requests.get(url.get('url'))
            logger.info("SIFMONITOR Server: {} is UP and running".format(url.get('host')))
            server_status.append(True)
        except requests.exceptions.ConnectionError as error:
            server_status.append(False)
            logger.info("SIFMONITOR Server: {} is DOWN and a new master has to be selected".format(url.get('host')))
            continue
    return server_status


def get_urls():
    sif_monitor_details = settings.SIFMONITOR_CREDENTIALS[settings.SERVER_NAME]
    if settings.SERVER_NAME == 'SIF_MONITOR_BACKUP1':
        url = [{"url": "http://{}:{}/ping".format(sif_monitor_details['SERVER_HOST'], sif_monitor_details['MASTER_SERVER_PORT']), "host": "MASTER"}]

    elif settings.SERVER_NAME == 'SIF_MONITOR_BACKUP2':
        url1 = "http://{}:{}/ping".format(sif_monitor_details['SERVER_HOST'], sif_monitor_details['MASTER_SERVER_PORT'])
        url2 = "http://{}:{}/ping".format(sif_monitor_details['SERVER_HOST'], sif_monitor_details['BACKUP_SERVER_PORT'])
        url = [{'url': url1, 'host': 'MASTER'}, {'url': url2, 'host': 'BACKUP1'}]

    return url


def start_provision_aggregator():
    logger.info("Starting provision aggregator!!!")
    # Python_bin is path of your virtual environment
    python_bin = r""
    # script_file is provision_aggregator path
    script_file = r""
    subprocess.call([python_bin, script_file])

while 1:
    response = ping_server()
    if not any(response):
        start_provision_aggregator()
        break
    time.sleep(5)
