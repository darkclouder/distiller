from argparse import ArgumentParser

from distiller.worker.Worker import Worker


def get_action(conf, program):
    worker_parser = ArgumentParser(prog="%s %s" % (program, "worker"))
    worker_parser.add_argument("--start", required=True, action='store_const', const=True)
    worker_parser.add_argument(
        "--host",
        default=conf.get("distiller.socket.ip"),
        help="Host of core daemon, DEFAULT: %s (see config)" % conf.get("distiller.socket.ip")
    )
    worker_parser.add_argument(
        "--port",
        default=conf.get("distiller.socket.port"),
        help="Port of core daemon, DEFAULT: %s (see config)" % conf.get("distiller.socket.port")
    )

    def worker(args):
        if args.start:
            worker = Worker(args.host, args.port)
            worker.run_blocking()
        else:
            print("Invalid arguments")
            worker_parser.print_help()

    return {
        "desc": "Manage a worker process",
        "parser": worker_parser,
        "exec": worker
    }
