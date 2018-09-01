import sys

from distiller.api.DynamicClass import class_id
from distiller.api.AbstractTask import AbstractTask
from distiller.api.DataDriver import DataDriver
from distiller.api.Reader import Reader, ReadIterator
from distiller.utils.TaskLoader import TaskLoader
from distiller.drivers.internal.RowToBlobIterator import RowToBlobIterator


@class_id("DefaultPipe")
class DefaultPipe(AbstractTask):
    def stored_in(self):
        return PipeDriver()

    def pipe_iterator(self, input_readers):
        raise NotImplementedError

    def occurrences(self):
        return sys.maxsize


class PipeDriver(DataDriver):
    def delete_cask(self, spirit, config):
        raise ValueError("Pipe cannot be deleted")

    def write(self, spirit, config):
        raise ValueError("Pipe cannot be written to")

    def read(self, spirit, config):
        return PipeReader(spirit, config)


class PipeReader(Reader):
    def __init__(self, spirit, config):
        self.spirit = spirit
        self.config = config

    def it(self):
        return PipeIterator(self.spirit.pipe_iterator(TaskLoader.load_dependencies(self.spirit, self.config)))

    def blob(self):
        return RowToBlobIterator(self.it())


class PipeIterator(ReadIterator):
    def __init__(self, it):
        self.it = it
        self.entered = False

    def __iter__(self):
        return self.it()

    def __enter__(self):
        if self.entered:
            raise ValueError("Reader already open")
        self.entered = True

        return self

    def __exit__(self, type, value, traceback):
        self.entered = False
