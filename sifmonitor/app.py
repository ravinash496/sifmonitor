import falcon
import ping
import settings
from werkzeug.serving import run_simple

api = falcon.API()
api.add_route('/ping', ping.ping)


if __name__ == '__main__':

    run_simple(settings.SIFMONITOR_CREDENTIALS[settings.SERVER_NAME]['SERVER_HOST'],
               settings.SIFMONITOR_CREDENTIALS[settings.SERVER_NAME]['APP_SERVER_PORT'],
               api, use_debugger=True,
               use_reloader=True)
