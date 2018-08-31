class DataDriver:
    def read(self, spirit, config):
        """Get a data reader for a specific spirit.
        A data reader is a subclass of Reader

        Arguments:
        spirit -- The spirit to read from
        config -- System configuration
        """

        raise NotImplementedError

    def write(self, spirit, config):
        """Get a data writer for a specific spirit.
        A data writer is a subclass of Writer

        Arguments:
        spirit -- The spirit to write to
        config -- System configuration
        """

        raise NotImplementedError

    def delete_cask(self, spirit, config):
        """Delete the cask of a specific spirit.

        Arguments:
        spirit -- The spirit to write to
        config -- System configuration
        """

        raise NotImplementedError

    def delete_all_casks(self, config, whitelist=[]):
        """Delete all casks visible to a certain driver (even corrupt ones) [except whitelisted ones]

        Arguments:
        whitelist -- List of spirits not to delete, Default: []
        """


class WriteAfterCommitException(Exception):
    pass
