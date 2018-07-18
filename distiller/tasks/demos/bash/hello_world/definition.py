from distiller.api.DefaultStill import DefaultStill
from distiller.runners.BashRunner import BashRunner
from distiller.drivers.BinaryFileDriver import BinaryFileDriver


class Still(DefaultStill):
    def stored_in(self):
        return BinaryFileDriver(binary=True)

    def executed_by(self):
        return BashRunner("run.sh")
