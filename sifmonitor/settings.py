import json

CREDENTIAL_FILE = 'connection.json'


def read_json(json_file=CREDENTIAL_FILE):
    """Read JSON setting file"""
    try:
        with open(CREDENTIAL_FILE, 'r') as fp:
            return json.load(fp)
    except IOError as error:
        print(error)



        
