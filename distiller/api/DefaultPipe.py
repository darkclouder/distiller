import sys
import json

from .AbstractTask import AbstractTask


class DefaultPipe(AbstractTask):
    def stored_in(self):
        # TODO: pipe runner
        raise NotImplementedError

    def occurrences(self):
        return sys.maxsize
