from datetime import datetime
import os

from ..interfaces.Log import Log


class FileLog(Log):
    def __init__(self, env):
        self.env = env

        log_dir = self.env.config.get("log.dir_path", path=True)

        self.file_path = os.path.join(log_dir, "%s.log" % datetime.now().__str__())
        self.file = None
        self.has_logged = False

        if not os.path.exists(log_dir):
            os.makedirs(log_dir)

        self.file = open(self.file_path, "w")

    def __del__(self):
        if self.file is not None:
            self.file.close()

            if not self.has_logged:
                os.remove(self.file_path)

    def write(self, message):
        if self.file is None:
            raise RuntimeError

        self.file.write(message.replace("\n", " "))
        self.file.write("\n")

        self.has_logged = True


module_class = FileLog
