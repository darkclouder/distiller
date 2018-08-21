from distiller.api.DefaultStill import DefaultStill
from distiller.runners.BashRunner import BashRunner
from distiller.drivers.BinaryFileDriver import BinaryFileDriver


class Still(DefaultStill):
    def stored_in(self):
        return BinaryFileDriver(binary=True)

    def requires(self):
        return [
            ("demos.bash.hello_world", {}),
            ("demos.bash.hello_world", {}),
            ("demos.bash.hello_world", {"id": 1}),
        ]

    def executed_by(self):
        return BashRunner("run.sh", pipe_dependency=0)
