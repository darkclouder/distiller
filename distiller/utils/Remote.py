import requests
import tempfile
import os
import tarfile
import json


class Remote:
    def __init__(self, host, port):
        self.url_prefix = "http://%s:%i/" % (host, port)

    def run_next(self):
        url = self.url_prefix + "tasks/run"
        res = requests.post(url, "{}")

        if res.status_code != 200:
            raise NetworkError("%s: Status code %i" % (url, res.status_code))

        obj = res.json()

        if obj.get("error", None) is not None:
            raise RemoteError(obj["error"])

        return obj

    def download_tasks(self, target):
        url = self.url_prefix + "tasks/definitions.tar.gz"
        res = requests.get(url, stream=True)

        if res.status_code != 200:
            raise NetworkError("%s: Status code %i" % (url, res.status_code))

        _, tmp_file = tempfile.mkstemp()

        with open(tmp_file, "wb") as f:
            for chunk in res.iter_content(8192):
                f.write(chunk)

        with tarfile.open(tmp_file, "r:gz") as tar:
            tar.extractall(target)

        os.remove(tmp_file)

    def finish_task(self, transaction_id, finish_state, message):
        url = self.url_prefix + "tasks/finish/%i" % transaction_id

        if isinstance(message, Exception):
            message = message.__str__()

        res = requests.post(url, json.dumps({
            "status": finish_state.name,
            "message": message
        }))

        if res.status_code != 200:
            raise NetworkError("%s: Status code %i" % (url, res.status_code))

        obj = res.json()

        if obj.get("error", None) is not None:
            raise RemoteError(obj["error"])

        return obj

    def add_target(self, spirit_id, options=None):
        if options is None:
            options = {}

        url = self.url_prefix + "targets/add"

        res = requests.post(url, json.dumps({
            "spirit_id": spirit_id,
            "options": options
        }))

        if res.status_code != 200:
            raise NetworkError("%s: Status code %i" % (url, res.status_code))

        obj = res.json()

        if obj.get("error", None) is not None:
            raise RemoteError(obj["error"])

    def remove_target(self, spirit_id, options=None):
        if options is None:
            options = {}

        url = self.url_prefix + "targets/remove"

        res = requests.post(url, json.dumps({
            "spirit_id": spirit_id,
            "options": options
        }))

        if res.status_code != 200:
            raise NetworkError("%s: Status code %i" % (url, res.status_code))

        obj = res.json()

        if obj.get("error", None) is not None:
            raise RemoteError(obj["error"])

    def heartbeat(self, transaction_id):
        url = self.url_prefix + "tasks/heartbeat/%i" % transaction_id

        res = requests.post(url)

        if res.status_code != 200:
            raise NetworkError("%s: Status code %i" % (url, res.status_code))

        obj = res.json()

        if obj.get("error", None) is not None:
            raise RemoteError(obj["error"])

    def remove_spirit_cask(self, spirit_id):
        url = self.url_prefix + "casks/remove/spirit"

        res = requests.post(url, json.dumps({
            "spirit_id": spirit_id
        }))

        if res.status_code != 200:
            raise NetworkError("%s: Status code %i" % (url, res.status_code))

        obj = res.json()

        if obj.get("error", None) is not None:
            raise RemoteError(obj["error"])

    def remove_casks(self, mode):
        url = self.url_prefix + "casks/remove/" + mode

        res = requests.post(url)

        if res.status_code != 200:
            raise NetworkError("%s: Status code %i" % (url, res.status_code))

        obj = res.json()

        if obj.get("error", None) is not None:
            raise RemoteError(obj["error"])


class NetworkError(Exception):
    pass


class RemoteError(Exception):
    pass
