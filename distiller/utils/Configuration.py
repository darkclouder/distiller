import os
import sys
import json
import pkg_resources

from .PathFinder import PathFinder
from distiller.helpers.extend import extend


class Configuration:
    env_key = "DISTILLER_CONF_FILE"

    def __init__(self, conf_dict):
        self.conf_dict = conf_dict

    def get(self, key, default=None):
        path = key.split(".")

        curr = self.conf_dict

        for step in path:
            if step in curr:
                curr = curr[step]
            else:
                return default

        return curr

    def extend(self, new_conf):
        extend(self.conf_dict, new_conf.conf_dict)

    @classmethod
    def load(cls, override=None):
        conf = cls.load_default()

        try:
            conf.extend(cls.load_env())
        except IOError as e:
            print(e, file=sys.stderr)

        if override is not None:
            conf.extend(override)

        return conf

    @classmethod
    def load_default(cls):
        return cls.load_file(PathFinder.get_default_config())

    @classmethod
    def load_env(cls):
        env_path = PathFinder.get_config()
        if env_path is not None and os.path.isfile(env_path):
            return cls.load_file(env_path)

        return cls({})

    @classmethod
    def load_file(cls, file_path):
        with open(file_path) as f:
            return cls(json.load(f))
