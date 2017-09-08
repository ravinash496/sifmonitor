import json

CREDENTIAL_FILE = 'connection.json'
application_flag = 'application_flag.txt'
THREAD_SLEEP_TIME = 20
target_schema = "provisioning"
service_urn_file = "serviceurnlayermapping.py"

def read_json(credential_file):
    """Read JSON setting file"""
    try:
        with open(credential_file, 'r') as fp:
            return json.load(fp)
    except IOError as error:
        print(error)
