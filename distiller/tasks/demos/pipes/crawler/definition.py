import requests

from distiller.api.DefaultPipe import DefaultPipe


class CrawlerPipe(DefaultPipe):
    def default_parameters(self):
        return {
            "still_urls": None,
            "url_field": None
        }

    def requires(self):
        if self.parameters["still_urls"] is None:
            return []
        else:
            return [self.parameters["still_urls"]]

    def pipe_iterator(self, input_readers):
        url_field = self.parameters["url_field"]

        if self.parameters["still_urls"] is None:
            def it():
                pass
        else:
            def it():
                with input_readers[0].it() as r:
                    for row in r:
                        res = None

                        if url_field is None:
                            url = row
                        else:
                            url = row[url_field]

                        if url is not None and len(url) > 0:
                            if not url.startswith("http://") and not url.startswith("https://"):
                                url = "http://" + url

                            try:
                                res = requests.get(url, timeout=10)
                            except Exception as e:
                                print(e)

                        if res is not None and res.status_code == 200:
                            yield {
                                "meta": row,
                                "text": res.text
                            }

        return it
