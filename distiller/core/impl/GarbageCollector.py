from distiller.utils.DependencyExplorer import DependencyExplorer
from distiller.utils.TaskLoader import TaskLoader


class GarbageCollector:
    def __init__(self, env):
        self.env = env
        self.logger = self.env.logger.claim("GC")

    def delete_all(self, whitelist=[]):
        with self.env.scheduler.lock():
            running_spirits = self.__running_spirits()
            whitelist_labels = {spirit.label() for spirit in running_spirits}
            whitelist_labels.update([spirit.label() for spirit in whitelist])

            # # Delete actual cask data
            for driver in self.env.drivers:
                driver.delete_all_casks(self.env.config, whitelist=running_spirits)

            # # Delete meta data about cask
            for spirit in self.__cask_spirits():
                if spirit is not None and spirit.label() not in running_spirit_labels:
                    self.env.meta.invalidate_cask(spirit.spirit_id())

    def delete_corrupt(self):
        # Delete all casks that are not registered in the meta db,
        # is a pipe (now) or cannot be initiated from its still
        existing_casks = [spirit for spirit in self.__cask_spirits() if not TaskLoader.spirit_is_pipe(spirit)]

        for driver in self.env.drivers:
            driver.delete_all_casks(self.env.config, whitelist=existing_casks)

    def delete_spirit(self, spirit):
        spirit.stored_in().delete_cask(spirit, self.env.config)
        self.env.meta.invalidate_cask(spirit.spirit_id())

    def delete_unused(self):
        scheduled_spirits = [
            TaskLoader.init(info.spirit_id)
            for info in self.env.meta.get_scheduled_infos()
        ]

        self.delete_all(whitelist=scheduled_spirits)

    def __cask_spirits(self):
        return [
            TaskLoader.init(cask["spirit_id"], none_on_error=True)
            for cask in self.env.meta.get_all_casks()
        ]

    def __running_spirits(self):
        return {
            spirit_id
            for target_id in self.env.scheduler.get_active_targets()
            for spirit_id in DependencyExplorer.involved_spirits(target_id)
        }