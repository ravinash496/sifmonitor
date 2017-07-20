import json

CREDENTIAL_FILE = 'connection.json'
application_flag = 'application_flag.txt'
THREAD_SLEEP_TIME = 20
PG_CREDENTIAL_FILE = 'pg_connection.json'


def read_json(credential_file):
    """Read JSON setting file"""
    try:
        with open(credential_file, 'r') as fp:
            return json.load(fp)
    except IOError as error:
        print(error)
