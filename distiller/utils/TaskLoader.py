import os
import importlib.util
import inspect
import json

from distiller.utils.PathFinder import PathFinder
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

        # FIXME: why does this subclass check work here but for MongoDriver a workaround needs to be built?
        # FIXME: by creating a class_id.
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
    def init(cls, spirit_id, task_root=None, always_refresh=True, none_on_error=False):
        still_id, parameters = spirit_id

        if always_refresh:
            cls.invalidate(still_id)

        # TODO: this might be a bit inefficient to create json string for every access
        json_params = json.dumps(parameters, sort_keys=True)

        if still_id not in cls.cached_spirits:
            cls.cached_spirits[still_id] = {}

        if json_params in cls.cached_spirits[still_id]:
            return cls.cached_spirits[still_id][json_params]

        if none_on_error:
            try:
                spirit_instance = cls.load(still_id, task_root=task_root)(parameters)
            except TaskLoadError:
                return None
        else:
            spirit_instance = cls.load(still_id, task_root=task_root)(parameters)
        cls.cached_spirits[still_id][json_params] = spirit_instance

        return spirit_instance

    @classmethod
    def spirit_is_pipe(cls, spirit):
        return spirit.inherits("DefaultPipe")

    @classmethod
    def load_dependencies(cls, spirit, config, default_driver=None):
        if default_driver is None:
            driver_module = importlib.import_module(config.get("spirits.default_driver.module"))
            default_driver = driver_module.module_class(**config.get("spirits.default_driver.params"))

        deps = [TaskLoader.init(dep) for dep in spirit.requires()]

        return [
            default_driver.read(dep, config)
            if dep.stored_in() is None
            else dep.stored_in().read(dep, config)
            for dep in deps
        ]

class TaskLoadError(Exception):
    pass
