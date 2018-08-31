class WriteModes:
    def replace(self):
        """Write with replace strategy: All previous data is wiped and replaced with new data"""

        raise NotImplementedError

    def update(self, key):
        """Write with update strategy on existing data.
        For this, a key field must be provided to use for updating.
        Writing None to a key removes it from the cask"""

        raise NotImplementedError

    def append(self):
        """Write by appending.
        This does not remove existing data but appends it.
        """

        raise NotImplementedError


class Writer:
    def write(self, data):
        """Write a relational entry, or an entire blob"""
        raise NotImplementedError

    def commit(self):
        """Commits the change. Write operations after this lead to an error"""
        raise NotImplementedError

    def open(self):
        return self.__enter__()

    def close(self):
        return self.__exit__(None, None, None)

    def __enter__(self):
        raise NotImplementedError

    def __exit__(self, type, value, traceback):
        """If exit appears without a commit, undo all changes"""
        raise NotImplementedError


class WriteAfterCommitException(Exception):
    pass
