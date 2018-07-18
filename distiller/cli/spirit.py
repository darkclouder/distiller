from argparse import ArgumentParser
import json

from distiller.utils.Remote import Remote


def get_action(conf, program):
    spirit_parser = ArgumentParser(prog="%s %s" % (program, "spirit"))
    spirit_parser.add_argument(
        "action",
        help="Remote action, e.g.\n\tadd -- adds a target spirit to the scheduler"
    )
    spirit_parser.add_argument(
        "--host",
        default=conf.get("distiller.socket.ip"),
        help="Host of core daemon, DEFAULT: %s (see config)" % conf.get("distiller.socket.ip")
    )
    spirit_parser.add_argument(
        "--port",
        default=conf.get("distiller.socket.port"),
        help="Port of core daemon, DEFAULT: %s (see config)" % conf.get("distiller.socket.port")
    )
    # Spirit settings
    spirit_parser.add_argument(
        "still_id",
        help="Still id in dot notation"
    )
    spirit_parser.add_argument(
        "-p",
        default="{}",
        help="Spirit parameters"
    )
    spirit_parser.add_argument(
        "-o",
        default="{}",
        help="Scheduling options"
    )

    def spirit(args):
        remote = Remote(args.host, args.port)

        if args.action == "add":
            remote.add_target((args.still_id, json.loads(args.p)), options=json.loads(args.o))
        elif args.action == "remove":
            remote.remove_target((args.still_id, json.loads(args.p)), options=json.loads(args.o))
        else:
            print("Invalid action")
            spirit_parser.print_help()

    return {
        "desc": "Control spirits in daemon scheduler",
        "parser": spirit_parser,
        "exec": spirit
    }
