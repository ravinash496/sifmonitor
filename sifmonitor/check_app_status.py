import os
import requests
import subprocess
import settings
import time
from logger_settings import *

config = settings.read_config_file()
SERVER_NAME = config["App"]["SERVER_NAME"]


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
    sif_monitor_details = eval(config["App"]["SIFMONITOR_CREDENTIALS"])
    SERVER_HOST1 = sif_monitor_details[SERVER_NAME]["SERVER_HOST"]
    number_of_servers = list(sif_monitor_details.keys())
    urls_list = []
    if 'SIF_MONITOR_MASTER' == SERVER_NAME:
        logger.info("No URLs to fetch")

    elif 'BACKUP' in SERVER_NAME:
        server_list = eval(SERVER_NAME[-1])
        master_host = sif_monitor_details["SIF_MONITOR_MASTER"]["SERVER_HOST"]

        if server_list == 1:
            MASTER_SERVER_NAME = "SIF_MONITOR_MASTER"
            url1 = "http://{}:{}/ping".format(master_host, sif_monitor_details[SERVER_NAME]['MASTER_SERVER_PORT'])
            url = [{'url': url1, 'host': MASTER_SERVER_NAME}]

        elif server_list > 1:
            master_url = "http://{}:{}/ping".format(master_host, sif_monitor_details["SIF_MONITOR_MASTER"]['APP_SERVER_PORT'])
            urls_list.append({'url': master_url, 'host': 'SIF_MONITOR_MASTER'})

            for server in range(1, server_list):
                BACKUP_SERVER_NAME = "SIF_MONITOR_BACKUP" + str(server)
                backup_host = sif_monitor_details[BACKUP_SERVER_NAME]["SERVER_HOST"]
                url1 = "http://{}:{}/ping".format(backup_host, sif_monitor_details[BACKUP_SERVER_NAME]['APP_SERVER_PORT'])
                urls_list.append({'url': url1, 'host': BACKUP_SERVER_NAME})
                url1 = ""

            url = urls_list

    return url


def start_provision_aggregator():
    logger.info("Starting provision aggregator!!!")
    # Python_bin is path of your virtual environment
    venv = config["file_path"]["python_bin"]
    python_bin = r"{}".format(venv)
    # script_file is provision_aggregator path
    provision_file = config["file_path"]["script_file_provision_aggregator"]
    script_file = r"{}".format(provision_file)
    # subprocess.call([python_bin, script_file])
    subprocess.call([python_bin, script_file])


def start_app():
    logger.info("Starting app.py!!!")
    # Python_bin is path of your virtual environment
    venv = config["file_path"]["python_bin"]
    python_bin = r"{}".format(venv)
    # script_file is app.py path
    app_file = config["file_path"]["script_file_app"]
    script_file = r"{}".format(app_file)
    #  subprocess.Popen([python_bin, script_file1])
    subprocess.Popen([python_bin, script_file])

start_app()
while 1:
    if config["App"]["SERVER_NAME"] == 'SIF_MONITOR_MASTER':
        start_provision_aggregator()
    else:
        response = ping_server()
        if not any(response):
            start_provision_aggregator()
            break
    time.sleep(10)