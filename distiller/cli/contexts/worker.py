from argparse import ArgumentParser

from distiller.cli.cli_context import CliContext

from distiller.utils.Configuration import Configuration
from distiller.worker.Worker import Worker


def start_worker(program, args):
    conf = Configuration.load("worker")
    program_str = " ".join(program)

    worker_parser = ArgumentParser(prog="%s" % program_str)
    worker_parser.add_argument(
        "--host",
        default=conf.get("remote.default.ip"),
        help="Host of core daemon, DEFAULT: %s (see config)" % conf.get("remote.default.ip")
    )
    worker_parser.add_argument(
        "--port",
        default=conf.get("remote.default.port"),
        type=int,
        help="Port of core daemon, DEFAULT: %s (see config)" % conf.get("remote.default.port")
    )
    worker_parser.add_argument(
        "--auto-conf",
        default=False,
        action='store_const',
        const=True,
        help="Auto configuration: Configuration is fetched from daemon"
    )

    parsed_args = worker_parser.parse_args(args)

    worker = Worker(parsed_args.host, parsed_args.port, auto_conf=parsed_args.auto_conf)
    worker.run_blocking()


context = CliContext(
    "Manage a worker process",
    sub_contexts={
        "start": CliContext("Start a worker process", start_worker)
    }
)
