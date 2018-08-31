import signal
import sys

from distiller.cli.cli_context import CliContext

from distiller.utils.Environment import Environment


def start_daemon(program, args):
    if len(args) > 0:
        return True

    e = Environment()

    if e.distiller.is_running():
        print("\n\nDaemon already running... ABORT")
        sys.exit(1)

    signal.signal(signal.SIGINT, lambda x, y: e.distiller.stop())
    e.distiller.run()


context = CliContext(
    "Manage a daemon process",
    sub_contexts={
        "start": CliContext("Start a daemon process", start_daemon)
    }
)
