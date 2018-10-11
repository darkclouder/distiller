import re

from distiller.api.DefaultStill import DefaultStill
from distiller.drivers.CsvFileDriver import CsvFileDriver
from distiller.runners.PythonRunner import PythonRunner


class Still(DefaultStill):
    def stored_in(self):
        return CsvFileDriver(
            dict=True,
            fields=["name", "opening_hours"],
            file_params={"encoding": "utf-8"}
        )

    def default_parameters(self):
        return {
            "city": "Berlin"
        }

    def requires(self):
        return [(
            "demos.pipes.crawler",
            {
                "spirit_urls": ("demos.osm.get_amenity", {
                    "city": self.parameters["city"],
                    "amenity": "restaurant"
                }),
                "url_field": "website"
            }
        )]

    def executed_by(self):
        return PythonRunner(do)


def do(parameters, input_readers, output_writer):
    opening_hours = re.compile(
        "((mon?(tag)?|die?(nstag)?|mit?(twoch)?|don?(nerstag)?|fre?(itag)?|sam?(stag)?|son?(ntag)?)(\s|\W)*)+"
        "[0-9]{1,2}((\s|\W)*([:.](\s|\W)*[0-9]{2}))?"
        "((\s|\W)*[0-9]{1,2}((\s|\W)*([:.](\s|\W)*[0-9]{2}))?)?"
        "(\s*uhr)?",
        re.IGNORECASE
    )

    with output_writer.replace() as w:
        with input_readers[0].it() as r:
            for website in r:
                print(website["meta"]["website"])

                try:
                    clean_text = remove_html(website["text"])
                    hours = [match.group(0) for match in opening_hours.finditer(clean_text)]

                    print(hours)

                    if len(hours) > 0:
                        w.write({
                            "name": website["meta"]["name"],
                            "opening_hours": ",".join(hours)
                        })
                except Exception as e:
                    print("Parsing error", e)

            w.commit()


def remove_html(html):
    if html.count('"') % 2 != 0 or html.count("'") % 2 != 0:
        print("Invalid number of quotation marks")
        return ""

    one_line = html.replace("\n", " ").replace("\r", "")
    one_line = re.sub("<!--.*?-->", "", one_line)
    one_line = re.sub(
        "<\s*(script|style).*?>.*?<\/\s*(script|style)\s*>",
        "",
        one_line,
        flags=re.IGNORECASE
    )
    one_line = re.sub("<([^>\"']*|(\".*?\"|'.*?'))*>", "", one_line)
    one_line = re.sub("\s+", " ", one_line)

    return one_line
