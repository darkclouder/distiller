from argparse import ArgumentParser
import json
import sys

from distiller.cli.cli_context import CliContext

from distiller.utils.Remote import Remote
from distiller.utils.Configuration import Configuration


def get_parser(program, conf, spirit=False, options=False):
    program_str = " ".join(program)

    spirit_parser = ArgumentParser(prog=program_str)
    spirit_parser.add_argument(
        "--host",
        default=conf.get("remote.default.ip"),
        help="Host of core daemon, DEFAULT: %s (see config)" % conf.get("remote.default.ip")
    )
    spirit_parser.add_argument(
        "--port",
        default=conf.get("remote.default.port"),
        type=int,
        help="Port of core daemon, DEFAULT: %s (see config)" % conf.get("remote.default.port")
    )

    if spirit:
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

    if options:
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
        spirit_parser.add_argument(
            "--persistent",
            default=False,
            action='store_const',
            const=True,
            help="Persistent scheduling (scheduling option shortcut, does not override -o)"
        )

    return spirit_parser


def spirit_action(mode):
    def action(program, args):
        conf = Configuration.load("worker")

        spirit_parser = get_parser(program, conf, spirit=True, options=True)
        parsed_args = spirit_parser.parse_args(args)

        remote = Remote(parsed_args.host, parsed_args.port)

        options = json.loads(parsed_args.o)

        if "age_requirement" not in options:
            options["age_requirement"] = parsed_args.ar
        if "persistent" not in options:
            options["persistent"] = parsed_args.persistent

        if mode == "add":
            remote.add_target((parsed_args.still_id, json.loads(parsed_args.p)), options=options)
        elif mode == "remove":
            remote.remove_target((parsed_args.still_id, json.loads(parsed_args.p)), options=options)

    return action


def cask_delete_action(mode):
    def action(program, args):
        conf = Configuration.load("worker")
        program_str = " ".join(program)

        spirit_parser = ArgumentParser(prog=program_str)
        spirit_parser.add_argument(
            "--host",
            default=conf.get("remote.default.ip"),
            help="Host of core daemon, DEFAULT: %s (see config)" % conf.get("remote.default.ip")
        )
        spirit_parser.add_argument(
            "--port",
            default=conf.get("remote.default.port"),
            type=int,
            help="Port of core daemon, DEFAULT: %s (see config)" % conf.get("remote.default.port")
        )

        spirit_parser = get_parser(program, conf, spirit=mode == "spirit")

        if mode == "all":
            spirit_parser.add_argument(
                "--force",
                required=True,
                action='store_const',
                const=True,
                help="Force delete, without this flag 'remove all' fails"
            )

        parsed_args = spirit_parser.parse_args(args)

        if mode == "all" and not parsed_args.force:
            print("--force missing, all casks can only be deleted with --force")
            return sys.exit(1)

        remote = Remote(parsed_args.host, parsed_args.port)

        if mode == "spirit":
            remote.remove_spirit_cask((parsed_args.still_id, json.loads(parsed_args.p)))
        else:
            remote.remove_casks(mode)

    return action


context = CliContext(
    "Remotely manage distiller daemon",
    sub_contexts={
        "spirit": CliContext("(Un)schedule spirits on daemon", sub_contexts={
                "add": CliContext("Add a spirit to scheduler", spirit_action("add")),
                "remove": CliContext("Remove a spirit from scheduler", spirit_action("remove"))
            }
        ),
        "cask": CliContext("Remote access to spirit casks", sub_contexts={
                "delete": CliContext("Delete one/many casks to reduce disk usage", sub_contexts={
                    "spirit": CliContext("Delete cask of a single spirit", cask_delete_action("spirit")),
                    "corrupt": CliContext(
                        "Delete all casks with missing or corrupt still definition or corrupt meta info",
                        cask_delete_action("corrupt")
                    ),
                    "unused": CliContext(
                        "Delete all casks not part of a scheduled execution pipeline",
                        cask_delete_action("unused")
                    ),
                    "all": CliContext("Delete all casks (except currently active ones)", cask_delete_action("all")),
                })
            }
        )
    }
)
