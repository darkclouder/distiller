class DataDriver:
    def __init__(self, conf):
        self.conf = conf

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
