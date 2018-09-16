import random
import datetime

from distiller.api.DefaultStill import DefaultStill
from distiller.runners.PythonRunner import PythonRunner
from distiller.drivers.CsvFileDriver import CsvFileDriver


class Still(DefaultStill):
    def stored_in(self):
        return CsvFileDriver(
            dict=True,
            fields=["key", "value"],
            file_params={"encoding": "utf-8"}
        )

    def executed_by(self):
        return PythonRunner(do)


def do(parameters, input_readers, output_writer):
    with output_writer.update("key") as w:
        rows = ["a", "b", "c"]

        now = str(datetime.datetime.now())

        for row in rows:
            if random.random() < 0.5:
                data = {"key": row, "value": now}

                print(data)
                w.write(data)

        w.commit()