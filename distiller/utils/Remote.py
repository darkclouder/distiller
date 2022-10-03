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
            def is_within_directory(directory, target):
                
                abs_directory = os.path.abspath(directory)
                abs_target = os.path.abspath(target)
            
                prefix = os.path.commonprefix([abs_directory, abs_target])
                
                return prefix == abs_directory
            
            def safe_extract(tar, path=".", members=None, *, numeric_owner=False):
            
                for member in tar.getmembers():
                    member_path = os.path.join(path, member.name)
                    if not is_within_directory(path, member_path):
                        raise Exception("Attempted Path Traversal in Tar File")
            
                tar.extractall(path, members, numeric_owner=numeric_owner) 
                
            
            safe_extract(tar, target)

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

    def fetch_worker_conf(self):
        url = self.url_prefix + "config/accumulated/worker.json"

        res = requests.get(url)

        if res.status_code != 200:
            raise NetworkError("%s: Status code %i" % (url, res.status_code))

        return res.json()


class NetworkError(Exception):
    pass


class RemoteError(Exception):
    pass
