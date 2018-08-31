class Reader:
    def blob(self):
        """Returns a binary iterative reader
        Iterator is sublcass of ReadIterator
        """

        raise NotImplementedError

    def it(self):
        """Returns a relational row-based iterator
        Iterator is sublcass of ReadIterator
        """

        raise NotImplementedError


class ReadIterator:
    """This is an abstract iterator to be used for either type of blob or it iterator.
    The iterator should be opened/closed with a 'with' statement, and its object
    should then be able to iterate over.
    """

    def open(self):
        return self.__enter__()

    def close(self):
        return self.__exit__(None, None, None)

    def __enter__(self):
        raise NotImplementedError

    def __exit__(self, type, value, traceback):
        raise NotImplementedError

    def __iter__(self):
        raise NotImplementedError
