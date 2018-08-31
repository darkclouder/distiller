import os
import sys
import json

from .PathFinder import PathFinder
from distiller.helpers.extend import extend


class Configuration:
    def __init__(self, conf_dict):
        self.conf_dict = conf_dict

    def get(self, key, default=None, path=False):
        hierarchy = key.split(".")

        curr = self.conf_dict

        for level in hierarchy:
            if level in curr:
                curr = curr[level]
            else:
                curr = default
                break

        if path:
            return PathFinder.expand_path(curr)
        else:
            return curr

    def extend(self, new_conf):
        extend(self.conf_dict, new_conf.conf_dict)

    @classmethod
    def load(cls, mode="global", override=None):
        if mode == "global":
            conf = cls.load_default("global")
        else:
            conf = cls.load("global")
            conf.extend(cls.load_default(mode))

        try:
            conf.extend(cls.load_env(mode))
        except IOError as e:
            print(e, file=sys.stderr)

        if override is not None:
            extend(conf.conf_dict, override)

        return conf

    @classmethod
    def load_default(cls, mode="global"):
        return cls.load_file(PathFinder.get_default_config(mode))

    @classmethod
    def load_env(cls, mode="global"):
        env_path = PathFinder.get_config(mode)
        if env_path is not None and os.path.isfile(env_path):
            return cls.load_file(env_path)

        return cls({})

    @classmethod
    def load_file(cls, file_path):
        with open(file_path) as f:
            return cls(json.load(f))
