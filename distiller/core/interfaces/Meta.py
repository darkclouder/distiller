class Meta:
    def get_cask(self, spirit):
        """Returns cask information for spirit

        Arguments:
        spirit -- Spirit to get cask from
        """

        raise NotImplementedError

    def update_cask(self, spirit, completion=None):
        """Update cask information for spirit

        Arguments:
        spirit -- Spirit to update

        Keyword arguments:
        completion -- New complition date, None to set to now
        """

        raise NotImplementedError

    def get_scheduled_spirits(self):
        """Returns array of scheduling info of all (persistent) scheduled spirits"""

        raise NotImplementedError

    def remove_scheduled_spirit(self, schedule_id):
        """Remove (persistent) scheduled spirit"""

        raise NotImplementedError

    def add_scheduled_spirit(self, schedule_info):
        """Persistently save scheduling info"""

        raise NotImplementedError
