from argparse import ArgumentParser
import signal
import sys

from distiller.utils.Environment import Environment


def get_action(conf, program):
    worker_parser = ArgumentParser(prog="%s %s" % (program, "daemon"))
    worker_parser.add_argument("--start", required=True, action='store_const', const=True)

    def daemon(args):
        if args.start:
            e = Environment()

            if e.distiller.is_running():
                print("\n\nDaemon already running... ABORT")
                sys.exit(1)

            signal.signal(signal.SIGINT, lambda x, y: e.distiller.stop())

            e.distiller.run()
        else:
            print("Invalid arguments")
            worker_parser.print_help()

    return {
        "desc": "Manage a daemon process",
        "parser": worker_parser,
        "exec": daemon
    }
