from pymongo import MongoClient, ReplaceOne
import json
import hashlib
import importlib

from distiller.api.DataDriver import DataDriver
from distiller.api.Reader import Reader, ReadIterator
from distiller.api.Writer import Writer, WriteModes, WriteAfterCommitException
from distiller.api.DynamicClass import class_id
from distiller.drivers.internal.RowToBlobIterator import RowToBlobIterator


@class_id("MongoDriver")
class MongoDriver(DataDriver):
    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def read(self, spirit, config):
        return MongoReader(
            self.__collection(spirit),
            self.__credentials(config),
            **self.kwargs
        )

    def write(self, spirit, config):
        return MongoWriteModes(
            self.__collection(spirit),
            self.__credentials(config),
            **self.kwargs
        )

    def __collection(self, spirit):
        label = spirit.label()

        return (self.kwargs.get("collection_prefix", "") + label)[:50] + \
            "_" + hashlib.sha256(label.encode("utf-8")).hexdigest()

    def __credentials(self, config):
        if "credentials" in self.kwargs:
            return self.kwargs["credentials"]

        connection = self.kwargs.get("connection", config.get("drivers.settings.MongoDriver.default_connection"))
        return config.get("drivers.settings.MongoDriver.connections.%s" % connection)

    def delete_cask(self, spirit, config):
        collection = self.__collection(spirit)
        temp_collection = get_temp_collection(collection)

        credentials = self.__credentials(config)

        client = MongoClient(credentials["uri"])
        db = client[credentials["database"]]

        db.drop_collection(collection)
        db.drop_collection(temp_collection)

        client.close()

    def delete_all_casks(self, config, whitelist=None):
        if whitelist is None:
            whitelist = []

        whitelisted = {}

        # Load default driver
        # TODO: this is a bad hack, somehow stored_in needs to figure
        # out by itself what is the default driver, this also applies
        # to GC and worker.
        driver_module = importlib.import_module(config.get("spirits.default_driver.module"))
        default_driver = driver_module.module_class(**config.get("spirits.default_driver.params"))

        for spirit in whitelist:
            driver = spirit.stored_in()

            if driver is None:
                driver = default_driver

            if driver.inherits("MongoDriver"):
                connection_id = json.dumps(driver.__credentials(config), sort_keys=True)
                collection = driver.__collection(spirit)

                if connection_id not in whitelisted:
                    whitelisted[connection_id] = []

                whitelisted[connection_id].append(collection)

        connections = config.get("drivers.settings.MongoDriver.connections")

        # TODO: don't drop system collections, are there more?
        system_cols = ["system.indexes"]

        for connection in connections.values():
            connection_id = json.dumps(connection, sort_keys=True)

            whitelist_cols = whitelisted.get(connection_id, [])

            client = MongoClient(connection["uri"], connectTimeoutMS=5000)
            db = client[connection["database"]]

            drop_collections = set(db.collection_names()) \
                .difference(whitelist_cols) \
                .difference(system_cols)

            for coll in drop_collections:
                db.drop_collection(coll)

            client.close()


class MongoReader(Reader):
    def __init__(self, collection, credentials, **kwargs):
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
    def __init__(self, collection, credentials, **kwargs):
        self.collection = collection
        self.credentials = credentials
        self.kwargs = kwargs

    def replace(self):
        return MongoWriter(
            "replace",
            self.collection,
            self.credentials,
            **self.kwargs
        )

    def update(self, key):
        return MongoWriter(
            "update",
            self.collection,
            self.credentials,
            key=key,
            **self.kwargs
        )

    def append(self):
        return MongoWriter(
            "append",
            self.collection,
            self.credentials,
            **self.kwargs
        )


class MongoWriter(Writer):
    def __init__(self, mode, collection, credentials, key=None, **kwargs):
        self.committed = False
        self.mode = mode
        self.collection = collection
        self.credentials = credentials
        self.key = key
        self.client = None
        self.write_coll = None

        self.bulk_size = kwargs.get("bulk_size", 100)
        self.cached_rows = []

        if key is None and mode == "update":
            raise ValueError("Key cannot be None in update mode")

    def write(self, data):
        """Write a relational entry, or an entire blob"""

        if self.committed:
            raise WriteAfterCommitException

        data = data.copy()

        self.cached_rows.append(data)

        if len(self.cached_rows) >= self.bulk_size:
            self.__write_bulk()

    def __write_bulk(self):
        if self.mode == "update":
            self.write_coll.bulk_write([
                ReplaceOne({self.key: row[self.key]}, row, upsert=True)
                for row in self.cached_rows
            ])
        else:
            self.write_coll.insert_many(self.cached_rows, ordered=True)

        self.cached_rows = []

    def commit(self):
        """Commits the change. Write operations after this lead to an error"""

        if self.committed:
            raise WriteAfterCommitException

        if len(self.cached_rows) > 0:
            self.__write_bulk()

        self.committed = True

        self.client[self.credentials["database"]][self.collection].drop()
        self.write_coll.rename(self.collection)

        self.client.close()
        self.client = None
        self.write_coll = None

    def __enter__(self):
        self.client = MongoClient(self.credentials["uri"])

        if self.mode != "replace":
            self.client[self.credentials["database"]][self.collection].aggregate(
                [{"$out": get_temp_collection(self.collection)}]
            )

        self.write_coll = self.client[self.credentials["database"]][get_temp_collection(self.collection)]

        return self

    def __exit__(self, type, value, traceback):
        """If exit appears without a commit, undo all changes"""
        if not self.committed:
            self.write_coll.drop()

        if self.client is not None:
            self.client.close()
            self.client = None
            self.write_coll = None


def get_temp_collection(collection):
    return "~" + collection


module_class = MongoDriver
