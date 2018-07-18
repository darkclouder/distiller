from distiller.api.DefaultStill import DefaultStill
from distiller.runners.PythonRunner import PythonRunner
from distiller.drivers.CsvFileDriver import CsvFileDriver


class Still(DefaultStill):
    def stored_in(self):
        return CsvFileDriver(
            dict=True,
            fields=["city", "lat", "lon", "name"],
            file_params={"encoding": "utf-8"}
        )

    def executed_by(self):
        return PythonRunner(do)

    def default_parameters(self):
        return {"cities": ["Berlin", "München"]}

    def requires(self):
        return [
            ("demos.osm.crawl_bars", {"city": city})
            for city in self.parameters.get("cities", [])
        ]


def do(parameters, input_readers, output_writer):
    i = 0

    with output_writer.replace() as writer:
        for input_reader in input_readers:
            with input_reader.it() as reader:
                for row in reader:
                    row["city"] = parameters["cities"][i]
                    writer.write(row)

            i += 1

        writer.commit()
