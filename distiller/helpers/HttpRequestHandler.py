from http.server import BaseHTTPRequestHandler
import json
import sys


class HttpRequestHandler(BaseHTTPRequestHandler):

    max_length = 1024 * 1024

    def do_POST(self):
        try:
            con_len = int(self.headers.get("content-length", 0))
            con_len = max(0, min(con_len, HttpRequestHandler.max_length))
        except ValueError:
            self.error(411)
            self.end_headers()
            return

        handle = self.server.request_handler.resolve_post(self.path)

        if handle is None:
            self.error(404)
        else:
            body = self.rfile.read(con_len)

            if len(body) == 0:
                req = {}
            else:
                try:
                    req = json.loads(body.decode("utf-8"))
                except json.JSONDecodeError:
                    self.error(400)
                    return
                except Exception as e:
                    print(str(e), file=sys.stderr)
                    self.error(500)
                    return

            handle[0](self, handle[1], req)

    def do_GET(self):
        handle = self.server.request_handler.resolve_get(self.path)

        if handle is None:
            self.error(404)
        else:
            handle[0](self, handle[1])

    def error(self, code):
        self.send_response(code)
        self.send_header("content-type", "text/html")
        self.send_header("charset", "utf-8")
        self.end_headers()
        self.wfile.write(bytes("Error %i" % code, "utf8"))

    def json(self, obj):
        self.send_response(200)
        self.send_header("content-type", "application/json")
        self.send_header("charset", "utf-8")
        self.end_headers()
        self.wfile.write(bytes(json.dumps(obj), "utf8"))

    def text(self, message):
        self.send_response(200)
        self.send_header("content-type", "text/html")
        self.send_header("charset", "utf-8")
        self.end_headers()
        self.wfile.write(bytes(message, "utf8"))

    def log_request(self, code='-', size='-'):
        # silence log requests
        pass
