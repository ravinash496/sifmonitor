"""Ping the availability of App-Server"""
import settings
import falcon
import json

# Ping


class Ping:
    def on_get(self, req, res):
        data = {"status": "OK", "node": settings.SERVER_NAME}
        res.body = json.dumps(data)
        res.status = falcon.HTTP_200

ping = Ping()
