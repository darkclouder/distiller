import overpass

from distiller.api.DefaultStill import DefaultStill
from distiller.runners.PythonRunner import PythonRunner
from distiller.drivers.CsvFileDriver import CsvFileDriver


class Still(DefaultStill):
    def stored_in(self):
        return CsvFileDriver(
            dict=True,
            fields=["lat", "lon", "name", "website"],
            file_params={"encoding": "utf-8"}
        )

    def executed_by(self):
        return PythonRunner(do)

    def default_parameters(self):
        return {"city": "Berlin", "amenity": "restaurant"}

    def locks(self):
        return ["osm"]


def do(parameters, input_readers, output_writer):
    api = overpass.API()
    res = api.Get(
        'area[name="%s"]; node(area)[amenity=%s];' % (
            parameters["city"],
            parameters["amenity"]
        )
    )

    with output_writer.replace() as writer:
        for amenity in res.get("features", []):
            lat, lon = amenity.get("geometry", {}).get("coordinates", (None, None))
            properties = amenity.get("properties", {})

            writer.write({
                "lat": lat,
                "lon": lon,
                "name": properties.get("name", None),
                "website": properties.get("website", None)
            })

        writer.commit()
