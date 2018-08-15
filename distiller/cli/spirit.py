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
        default=conf.get("remote.default.ip"),
        help="Host of core daemon, DEFAULT: %s (see config)" % conf.get("remote.default.ip")
    )
    spirit_parser.add_argument(
        "--port",
        default=conf.get("remote.default.port"),
        help="Port of core daemon, DEFAULT: %s (see config)" % conf.get("remote.default.port")
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
    spirit_parser.add_argument(
        "--ar",
        default=None,
        type=int,
        help="Age requirement (scheduling option shortcut, does not override -o)"
    )

    def spirit(args):
        remote = Remote(args.host, args.port)

        options = json.loads(args.o)

        if "age_requirement" not in options:
            options["age_requirement"] = args.ar

        if args.action == "add":
            remote.add_target((args.still_id, json.loads(args.p)), options=options)
        elif args.action == "remove":
            remote.remove_target((args.still_id, json.loads(args.p)), options=options)
        else:
            print("Invalid action")
            spirit_parser.print_help()

    return {
        "desc": "Control spirits in daemon scheduler",
        "parser": spirit_parser,
        "exec": spirit
    }
