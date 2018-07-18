import json

from .AbstractTask import AbstractTask


class DefaultStill(AbstractTask):
    def stored_in(self):
        return None

    def occurrences(self):
        return 1

    def requires(self):
        return []
