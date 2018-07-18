import os


class PathFinder:
    conf_env = "DISTILLER_CONF_FILE"
    task_env = "DISTILLER_TASK_PATH"
    data_env = "DISTILLER_DATA_PATH"

    @classmethod
    def get_src_root(cls):
        return os.path.realpath(os.path.join(
            os.path.dirname(__file__),
            ".."
        ))

    @classmethod
    def get_default_config(cls):
        return os.path.realpath(os.path.join(cls.get_src_root(), "default_config.json"))

    @classmethod
    def get_config(cls):
        if cls.conf_env in os.environ:
            return os.environ[cls.conf_env]

        return None

    @classmethod
    def get_task_root(cls):
        if cls.task_env in os.environ:
            return os.environ[cls.task_env]

        return os.path.join(cls.get_src_root(), "tasks")

    @classmethod
    def get_task_path(cls, task_id, relative=False, task_root=None):
        if relative:
            return task_id.replace(".", "/")

        if task_root is None:
            task_root = cls.get_task_root()

        return os.path.join(task_root, task_id.replace(".", "/"))

    @classmethod
    def get_data_root(cls):
        if cls.data_env in os.environ:
            return os.environ[cls.data_env]

        return "data"

    @classmethod
    def get_data_path(cls, task_id):
        return cls.get_task_path(task_id, task_root=cls.get_data_root())
