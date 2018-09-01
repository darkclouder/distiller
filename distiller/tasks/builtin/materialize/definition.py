from distiller.api.DefaultStill import DefaultStill
from distiller.runners.PythonRunner import PythonRunner


class MaterializeStill(DefaultStill):
    def default_parameters(self):
        return {
            "pipe": None
        }
        # TODO: custom driver, but this requires a driver loader from a string to be able to init a driver from json

    def requires(self):
        if self.parameters["pipe"] is None:
            return []
        else:
            return [self.parameters["pipe"]]

    def executed_by(self):
        return PythonRunner(materialize)


def materialize(parameters, input_readers, output_writer):
    if parameters["pipe"] is not None:
        with output_writer.replace() as w:
            with input_readers[0].it() as r:
                for row in r:
                    w.write(row)
                w.commit()