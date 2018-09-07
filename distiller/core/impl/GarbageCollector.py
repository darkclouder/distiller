import importlib

from distiller.utils.DependencyExplorer import DependencyExplorer
from distiller.utils.TaskLoader import TaskLoader


class GarbageCollector:
    def __init__(self, env):
        self.env = env
        self.logger = self.env.logger.claim("GC")

        # Load default driver
        driver_module = importlib.import_module(self.env.config.get("spirits.default_driver.module"))
        self.default_driver = driver_module.module_class(**self.env.config.get("spirits.default_driver.params"))

    def delete_all(self, whitelist=None):
        if whitelist is None:
            self.logger.notice("Delete all requested")

        with self.env.scheduler.lock():
            if whitelist is None:
                whitelist = set()
            else:
                whitelist = set(whitelist)
                whitelist.update(set(self.__running_spirits()))

            whitelist_labels = {spirit.label() for spirit in whitelist}

            # # Delete actual cask data
            for driver in self.env.drivers:
                self.logger.notice("Delete casks for driver %s" % repr(driver))

                with self.logger.catch(Exception).warning():
                    driver.delete_all_casks(self.env.config, whitelist=whitelist)

            # # Delete meta about cask
            cask_spirits = self.__cask_spirits()

            for spirit in cask_spirits:
                if spirit is not None and spirit.label() not in whitelist_labels:
                    self.env.meta.invalidate_cask(spirit.spirit_id())

            self.logger.notice("Deleted %i cask(s) from meta db" % len(cask_spirits))

    def delete_corrupt(self):
        self.logger.notice("Delete corrupt requested")

        # Delete all casks that are not registered in the meta db,
        # is a pipe (now) or cannot be initiated from its still
        existing_casks = [spirit for spirit in self.__cask_spirits() if not TaskLoader.spirit_is_pipe(spirit)]

        self.delete_all(whitelist=existing_casks)

    def delete_spirit(self, spirit):
        self.logger.notice("Delete spirit requested: %s" % spirit)

        with self.env.scheduler.lock():
            running_spirits = self.__running_spirits()

            if spirit in running_spirits:
                raise ValueError("Cannot delete running spirit")

            driver = spirit.stored_in()

            if driver is None:
                driver = self.default_driver

            driver.delete_cask(spirit, self.env.config)
            self.env.meta.invalidate_cask(spirit.spirit_id())

    def delete_unused(self):
        self.logger.notice("Delete unused requested")
        # TODO: whitelist non-permanent scheduled ones?

        scheduled_spirits = {
            spirit
            for info in self.env.meta.get_scheduled_infos()
            for spirit in DependencyExplorer.involved_spirits(info.spirit_id)
        }

        self.delete_all(whitelist=scheduled_spirits)

    def __cask_spirits(self):
        return [
            TaskLoader.init(cask["spirit_id"], none_on_error=True)
            for cask in self.env.meta.get_all_casks()
        ]

    def __running_spirits(self):
        return {
            spirit
            for target_id in self.env.scheduler.get_active_targets()
            for spirit in DependencyExplorer.involved_spirits(target_id)
        }
