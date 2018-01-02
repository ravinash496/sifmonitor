"""Ping the availability of App-Server"""
import settings
import falcon
import json
config = settings.read_config_file()
# Ping
SERVER_NAME = config["App"]["SERVER_NAME"]


class Ping:
    def on_get(self, req, res):
        data = {"status": "OK", "node": SERVER_NAME}
        res.body = json.dumps(data)
        res.status = falcon.HTTP_200

ping = Ping()
