import sys

from distiller.utils.Configuration import Configuration

import distiller.cli.worker as util_worker
import distiller.cli.spirit as util_spirit
import distiller.cli.daemon as util_daemon


def global_help(context):
    print("usage: %s <command> [<args>]" % context["program"])
    print("\nCommands:")
    for (cmd, action) in context["actions"].items():
        print("%s - %s" % (cmd, action["desc"]))

    print(
        "\nFor detailed help of each command enter {0} <command> --help,\ne.g. {0} {1} --help".format(
            context["program"], next(iter(context["actions"]))
        )
    )


def main():
    worker_conf = Configuration.load("worker")
    daemon_conf = Configuration.load("daemon")

    program = sys.argv[0]
    actions = {
        "worker": util_worker.get_action(worker_conf, program),
        "spirit": util_spirit.get_action(worker_conf, program),
        "daemon": util_daemon.get_action(daemon_conf, program),
    }

    context = {
        "program": program,
        "actions": actions
    }

    if len(sys.argv) >= 2:
        cmd = sys.argv[1]
        other_args = sys.argv[2:]

        if cmd in context["actions"]:
            action = context["actions"][cmd]
            action["exec"](action["parser"].parse_args(other_args))
        elif cmd == "-h" or cmd == "--help":
            global_help(context)
        else:
            print("Invalid command %s" % cmd)
            global_help(context)
    else:
        global_help(context)

if __name__ == "__main__":
    main()
