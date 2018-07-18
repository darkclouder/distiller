import os
import importlib.util
import inspect
import json

from .PathFinder import PathFinder
from distiller.api.AbstractTask import AbstractTask


class TaskLoader:
    cached_tasks = {}
    cached_spirits = {}

    @classmethod
    def load(cls, task_id, task_root=None):
        if task_id in cls.cached_tasks:
            return cls.cached_tasks[task_id]

        task_path = PathFinder.get_task_path(task_id, task_root=task_root)
        task_def = os.path.join(task_path, "definition.py")

        if not os.path.isdir(task_path):
            raise TaskLoadError("Task %s not found" % task_id)

        if not os.path.isfile(task_def):
            raise TaskLoadError("Definition file for task %s missing" % task_id)

        spec = importlib.util.spec_from_file_location(task_id, task_def)
        task_module = importlib.util.module_from_spec(spec)

        try:
            spec.loader.exec_module(task_module)
        except:
            raise TaskLoadError("Definition file for task %s is corrupt" % task_id)

        task_classes = [
            v
            for (_, v) in inspect.getmembers(task_module, inspect.isclass)
            if v.__module__ == task_module.__name__ and issubclass(v, AbstractTask)
        ]

        if len(task_classes) == 0:
            raise TaskLoadError("Definition file for task %s does not contain a task" % task_id)

        cls.cached_tasks[task_id] = task_classes[0]

        return task_classes[0]

    @classmethod
    def invalidate(cls, task_id):
        if task_id in cls.cached_tasks:
            del cls.cached_tasks[task_id]

        if task_id in cls.cached_spirits:
            del cls.cached_spirits[task_id]

    @classmethod
    def init(cls, spirit, task_root=None, always_refresh=True):
        if always_refresh:
            cls.invalidate(spirit[0])

        # TODO: this might be a bit inefficient to create json string for every access
        json_params = json.dumps(spirit[1], sort_keys=True)

        if spirit[0] not in cls.cached_spirits:
            cls.cached_spirits[spirit[0]] = {}

        if json_params in cls.cached_spirits[spirit[0]]:
            return cls.cached_spirits[spirit[0]][json_params]

        spirit_instance = cls.load(spirit[0], task_root=task_root)(spirit[1])
        cls.cached_spirits[spirit[0]][json_params] = spirit_instance

        return spirit_instance


class TaskLoadError(Exception):
    pass
