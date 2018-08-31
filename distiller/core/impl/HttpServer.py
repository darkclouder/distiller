from http.server import HTTPServer
import time
import threading

from distiller.helpers.HttpRequestHandler import HttpRequestHandler


class HttpServer:
    poll_interval = 0.5

    def __init__(self, request_handler, env):
        self.env = env
        self.request_handler = request_handler
        self.server = None

    def run(self):
        sock = (
            self.env.config.get("distiller.socket.ip"),
            self.env.config.get("distiller.socket.port")
        )

        self.server = HTTPServer(sock, HttpRequestHandler)
        self.server.request_handler = self.request_handler
        self.server.env = self.env

        srv_thread = threading.Thread(target=self.__run_thread)
        srv_thread.start()

    def __run_thread(self):
        self.server.serve_forever(poll_interval=HttpServer.poll_interval)

    def stop(self):
        if self.server is not None:
            self.server.shutdown()
