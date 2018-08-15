import os
import re
import base64

from distiller.api.DataDriver import DataDriver
from distiller.utils.PathFinder import PathFinder


class FileDriver(DataDriver):
    cask_pattern = re.compile("[a-zA-Z0-9-_.]+\([a-zA-Z0-9-_.]*\)_([A-Za-z0-9+/=]+)")
    simplify_pattern = re.compile("[^a-zA-Z0-9-_.()]+")

    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def _get_data_path(self, spirit, config, create_path=False):
        task_path = PathFinder.get_task_path(
            spirit.name(),
            task_root=config.get("drivers.FileDriver.cask_path", path=True)
        )
        parameter_id = self._simplify_file_name(spirit.label())

        if create_path and not os.path.exists(task_path):
            os.makedirs(task_path)

        return os.path.join(task_path, parameter_id)

    def _simplify_file_name(self, name):
        encoded_name = base64.b64encode(name.encode("utf-8")).decode("ascii")
        return self.simplify_pattern.sub("_", name) + "_" + encoded_name

    def delete_cask(self, spirit, config):
        data_path = self._get_data_path(spirit, config)
        temp_path = get_temp_path(data_path)

        if os.path.exists(data_path):
            os.remove(data_path)

        if os.path.exists(temp_path):
            os.remove(temp_path)

    def discover_casks(self, config):
        root_path = config.get("drivers.FileDriver.cask_path", path=True)

        if os.path.exists(root_path):
            casks = []

            for root, _, files in os.walk(root_path):
                for file in files:
                    match = self.cask_pattern.match(file)

                    if match:
                        casks.append((
                            os.path.join(os.path.join(root, file)),
                            file,
                            base64.b64decode(match.group(1))
                        ))

            return casks
        else:
            return []


def split_file_param(file):
    return file.split("(", 1)


def get_temp_path(data_path):
    return data_path + "~"
