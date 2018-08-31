class Meta:
    def get_cask(self, spirit_id):
        """Returns cask information for spirit

        Arguments:
        spirit -- Spirit id to get cask from
        """

        raise NotImplementedError

    def get_all_casks(self):
        """Returns all cask information that is available as an array
        """

    def update_cask(self, spirit_id, completion=None):
        """Update cask information for spirit

        Arguments:
        spirit -- Spirit id to update

        Keyword arguments:
        completion -- New completion date, None to set to now
        """

        raise NotImplementedError

    def invalidate_cask(self, spirit_id):
        """Invalidate cask
        This usually removes all meta data about a spirit

        Arguments:
        spirit -- Spirit id to invalidate cask for
        """

        raise NotImplementedError

    def get_scheduled_infos(self):
        """Returns array of scheduling info of all (persistent) scheduled spirits"""

        raise NotImplementedError

    def remove_scheduled_spirit(self, schedule_id):
        """Remove (persistent) scheduled spirit"""

        raise NotImplementedError

    def add_scheduled_spirit(self, schedule_info):
        """Persistently save scheduling info"""

        raise NotImplementedError
