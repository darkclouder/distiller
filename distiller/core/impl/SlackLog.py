import requests
import json
import sys

from ..interfaces.Log import Log


class SlackLog(Log):
    def __init__(self, env):
        self.env = env
        self.url = self.env.config.get("log.webhook_url", path=True)

    def __del__(self):
        pass

    def write(self, message):
        resp = requests.post(self.url, {
            "payload": json.dumps({
                "text": message
            })
        })

        if resp.status_code != 200:
            print(message, file=sys.stderr)
            print(
                "SlackLog failed with status code %i" % resp.status_code,
                file=sys.stderr
            )


module_class = SlackLog
