import importlib

from distiller.core.Logger import Logger
from distiller.core.Distiller import Distiller
from distiller.core.impl.Watchdog import Watchdog

from .Configuration import Configuration


class Environment:
    def __init__(self, conf=Configuration.load("daemon")):
        self.config = conf

        self.__init_logger()
        self.__init_distiller()

    def __init_logger(self):
        log_module = importlib.import_module(self.config.get("log.module"))
        self.logger = Logger(log_module.module_class(self), self)

    def __init_distiller(self):
        self.__init_scheduler()
        self.__init_watchdog()

        self.distiller = Distiller(self)

    def __init_scheduler(self):
        self.__init_meta()

        scheduler_module = importlib.import_module(self.config.get("scheduler.module"))
        self.scheduler = scheduler_module.module_class(self)

    def __init_meta(self):
        meta_module = importlib.import_module(self.config.get("meta.module"))

        self.meta = meta_module.module_class(self)

    def __init_watchdog(self):
        self.watchdog = Watchdog(self)
