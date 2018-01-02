import falcon
import ping
import settings
from werkzeug.serving import run_simple

config = settings.read_config_file()
api = falcon.API()
api.add_route('/ping', ping.ping)


if __name__ == '__main__':
    SERVER_NAME = config["App"]["SERVER_NAME"]
    run_simple(eval(config["App"]["SIFMONITOR_CREDENTIALS"])[SERVER_NAME]["SERVER_HOST"],
               eval(config["App"]["SIFMONITOR_CREDENTIALS"])[SERVER_NAME]['APP_SERVER_PORT'],
               api, use_debugger=True,
               use_reloader=True)

