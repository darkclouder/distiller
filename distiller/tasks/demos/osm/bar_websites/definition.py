from distiller.api.DefaultStill import DefaultStill
from distiller.runners.PythonRunner import PythonRunner
from distiller.drivers.CsvFileDriver import CsvFileDriver


class MaterializeStill(DefaultStill):
    def stored_in(self):
        return CsvFileDriver(
            dict=True,
            fields=["text"],
            file_params={"encoding": "utf-8"}
        )

    def default_parameters(self):
        return {}

    def requires(self):
        return [
            ("demos.pipes.crawler", {"still_urls": ("demos.osm.crawl_bars", self.parameters), "url_field": "website"})
        ]

    def executed_by(self):
        return PythonRunner(materialize)


def materialize(parameters, input_readers, output_writer):
    with output_writer.replace() as w:
        with input_readers[0].it() as r:
            for row in r:
                w.write(row)
            w.commit()
