import os

from distiller.core.impl.HttpServer import HttpServer
from distiller.core.impl.CoreHandler import CoreHandler


class Distiller:
    def __init__(self, env):
        self.env = env
        self.logger = self.env.logger.claim("Core")
        self.shutdown = False

        self.srv = HttpServer(CoreHandler(), self.env)
        self.pidfile = self.env.config.get("distiller.pidfile", path=True)

    def is_running(self):
        # Check if pid file already exists
        # and if the pid is still running
        if os.path.isfile(self.pidfile):
            with open(self.pidfile, "r") as f:
                try:
                    pid = int(f.readline())
                except ValueError:
                    self.logger.warning("Corrupt pid file")
                    os.remove(self.pidfile)
                    return False

                # Check if process still running
                try:
                    os.kill(pid, 0)
                except OSError:
                    self.logger.notice("Daemon not running, but pid file exists")
                    os.remove(self.pidfile)
                    return False
                else:
                    return True

        return False

    def run(self):
        self.logger.notice("Daemon start-up")

        # Write pid to pidfile
        pid = str(os.getpid())

        with open(self.pidfile, "w") as f:
            f.write(pid)

        # Start watchdog (non-blocking)
        self.env.watchdog.run()

        # Start web server (blocking)
        self.srv.run()

    def stop(self):
        self.logger.notice("Daemon shutdown initiated")

        # Stop web server
        self.srv.stop()

        # Stop watchdog (non-blocking)
        self.env.watchdog.stop()

        os.remove(self.pidfile)

        self.logger.notice("Daemon shutdown done")
