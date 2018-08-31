import sys

from .AbstractTask import AbstractTask


class DefaultPipe(AbstractTask):
    def stored_in(self):
        # TODO: pipe runner
        raise NotImplementedError

    def occurrences(self):
        return sys.maxsize

    def class_id(self):
        return "DefaultPipe"
