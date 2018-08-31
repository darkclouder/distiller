import importlib
import os

from distiller.core.Logger import Logger
from distiller.core.Distiller import Distiller
from distiller.core.impl.Watchdog import Watchdog
from distiller.core.impl.GarbageCollector import GarbageCollector

from distiller.utils.Configuration import Configuration


class Environment:
    def __init__(self, conf=None):
        if conf is None:
            conf = Configuration.load("daemon")

        self.config = conf

        self.__init_logger()
        self.__init_drivers()
        self.__init_distiller()

    def __init_logger(self):
        log_module = importlib.import_module(self.config.get("log.module"))
        self.logger = Logger(log_module.module_class(self), self)

    def __init_drivers(self):
        log = self.logger.claim("Env")

        self.drivers = []

        # Load internal drivers
        drivers_path = os.path.dirname(importlib.util.find_spec("distiller.drivers").origin)

        for file in os.listdir(drivers_path):
            driver_path = os.path.join(drivers_path, file)
            if file.endswith(".py") and \
                    not file.startswith("__") and \
                    os.path.isfile(driver_path):
                driver_name = "distiller.drivers.%s" % file[:-3]
                try:
                    spec = importlib.util.spec_from_file_location(driver_name, driver_path)
                    driver_module = importlib.util.module_from_spec(spec)
                    spec.loader.exec_module(driver_module)
                    self.drivers.append(driver_module.module_class())
                except BaseException as e:
                    log.error("Driver %s corrupt, %s" % (driver_name, e))

        # Load external drivers
        for driver_module_path in self.config.get("drivers.external", []):
            try:
                driver_module = importlib.import_module(driver_module_path)
                self.drivers.append(driver_module.module_class())
            except ModuleNotFoundError:
                log.error("Driver %s not found" % driver_module_path)
            except BaseException as e:
                log.error("Driver %s corrupt, %s" % (driver_module_path, e))

        log.notice("%i drivers loaded" % len(self.drivers))

    def __init_distiller(self):
        self.__init_scheduler()
        self.__init_watchdog()

        self.gc = GarbageCollector(self)
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
