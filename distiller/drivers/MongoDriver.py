from distiller.api.DataDriver import DataDriver
from distiller.api.Reader import Reader, ReadIterator
from distiller.api.Writer import Writer, WriteModes, WriteAfterCommitException
from distiller.drivers.internal.RowToBlobIterator import RowToBlobIterator

from pymongo import MongoClient


class MongoDriver(DataDriver):
    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def read(self, spirit, config):
        connection = self.kwargs.get("connection", config.get("drivers.MongoDriver.default_connection"))
        credentials = config.get("drivers.MongoDriver.connections.%s" % connection)

        return MongoReader(self.__collection(spirit), credentials)

    def write(self, spirit, config):
        connection = self.kwargs.get("connection", config.get("drivers.MongoDriver.default_connection"))
        credentials = config.get("drivers.MongoDriver.connections.%s" % connection)

        return MongoWriteModes(self.__collection(spirit), credentials)

    def __collection(self, spirit):
        return self.kwargs.get("collection_prefix", "") + spirit.label()


class MongoReader(Reader):
    def __init__(self, collection, credentials):
        self.collection = collection
        self.credentials = credentials

    def blob(self):
        return RowToBlobIterator(self.it())

    def it(self):
        return RowIterator(self.collection, self.credentials)


class RowIterator(ReadIterator):
    def __init__(self, collection, credentials):
        self.collection = collection
        self.credentials = credentials
        self.client = None

    def __enter__(self):
        self.client = MongoClient(self.credentials["uri"])
        self.it = self.client[self.credentials["database"]][self.collection].find()

        return self

    def __exit__(self, type, value, traceback):
        if self.client is not None:
            self.client.close()
            self.client = None
            self.it = None

    def __iter__(self):
        if self.client is None:
            raise RuntimeError("Must be entered with with-statement before usage")

        for row in self.it:
            yield row


class MongoWriteModes(WriteModes):
    def __init__(self, collection, credentials):
        self.collection = collection
        self.credentials = credentials

    def replace(self):
        return MongoWriter("replace", self.collection, self.credentials)

    def update(self, key):
        return MongoWriter("update", self.collection, self.credentials, key)

    def append(self):
        return MongoWriter("append", self.collection, self.credentials)


class MongoWriter(Writer):
    def __init__(self, mode, collection, credentials, key=None):
        self.commited = False
        self.mode = mode
        self.collection = collection
        self.credentials = credentials
        self.key = key
        self.client = None

        if key is None and mode == "update":
            raise ValueError("Key cannot be None in update mode")

    def write(self, data):
        """Write a relational entry, or an entire blob"""

        data = data.copy()

        if self.commited:
            raise WriteAfterCommitException

        if self.mode == "update":
            self.write_coll.replace_one({self.key: data[self.key]}, data, True)
        else:
            self.write_coll.insert_one(data)

    def commit(self):
        """Commits the change. Write operations after this lead to an error"""

        if self.commited:
            raise WriteAfterCommitException

        self.commited = True

        self.client[self.credentials["database"]][self.collection].drop()
        self.write_coll.rename(self.collection)

        self.client.close()
        self.client = None
        self.write_coll = None

    def __enter__(self):
        self.client = MongoClient(self.credentials["uri"])

        if self.mode != "update":
            self.client[self.credentials["database"]][self.collection].aggregate([{"$out": "~" + self.collection}])

        self.write_coll = self.client[self.credentials["database"]]["~" + self.collection]

        return self

    def __exit__(self, type, value, traceback):
        """If exit appears without a commit, undo all changes"""
        if not self.commited:
            self.write_coll.drop()

        if self.client is not None:
            self.client.close()
            self.client = None
            self.write_coll = None


module_class = MongoDriver
