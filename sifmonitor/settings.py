import json

CREDENTIAL_FILE = 'connection.json'
PG_CREDENTIAL_FILE = 'pg_connection.json'


def read_json(credential_file):
    """Read JSON setting file"""
    try:
        with open(credential_file, 'r') as fp:
            return json.load(fp)
    except IOError as error:
        print(error)
