import sys

from distiller.api.DefaultStill import DefaultStill


class Still(DefaultStill):
    def __init__(self, parameters=None):
        super().__init__(parameters)

    def default_parameters(self):
        return {"requires": []}

    def requires(self):
        return self.parameters["requires"]

    def occurrences(self):
        return sys.maxsize