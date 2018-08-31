import re


class RequestHandler:
    def __init__(self):
        self.routes = {"get": [], "post": []}

    def get(self, route, func):
        self.routes["get"].append((re.compile(route), func))

    def post(self, route, func):
        self.routes["post"].append((re.compile(route), func))

    def all(self, route, func):
        self.get(route, func)
        self.post(route, func)

    def resolve_get(self, url):
        return self.resolve_any("get", url)

    def resolve_post(self, url):
        return self.resolve_any("post", url)

    def resolve_any(self, req_type, url):
        # TODO: make this more efficient (sub-linear) with some prefix patterns maybe?

        for (pattern, func) in self.routes[req_type]:
            match = pattern.fullmatch(url)

            if match is not None:
                return func, match.groupdict()

        return None
