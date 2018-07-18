import os

from .impl.SimpleScheduler import SimpleScheduler
from .impl.HttpServer import HttpServer
from .impl.CoreHandler import CoreHandler


class Distiller:
    def __init__(self, env):
        self.env = env
        self.logger = self.env.logger.claim("Core")
        self.shutdown = False

        self.srv = HttpServer(CoreHandler(), self.env)

    def is_running(self):
        pidfile = self.env.config.get("distiller.pidfile")

        # Check if pid file already exists
        # and if the pid is still running
        if os.path.isfile(pidfile):
            with open(pidfile, "r") as f:
                try:
                    pid = int(f.readline())
                except ValueError:
                    self.logger.warning("Corrupt pid file")
                    os.remove(pidfile)
                    return False

                # Check if process still running
                try:
                    os.kill(pid, 0)
                except OSError:
                    self.logger.notice("Deamon not running, but pid file exists")
                    os.remove(pidfile)
                    return False
                else:
                    return True

        return False

    def run(self):
        self.logger.notice("Daemon start-up")

        # Write pid to pidfile
        pidfile = self.env.config.get("distiller.pidfile")
        pid = str(os.getpid())

        with open(pidfile, "w") as f:
            f.write(pid)

        # Start watchdog (non-blocking)
        self.env.watchdog.run()

        # Start web server (blocking)
        self.srv.run()

    def stop(self):
        pidfile = self.env.config.get("distiller.pidfile")

        self.logger.notice("Daemon shutdown initiated")

        # Stop web server
        self.srv.stop()

        # Stop watchdog (non-blocking)
        self.env.watchdog.stop()

        os.remove(pidfile)

        self.logger.notice("Daemon shutdown done")
