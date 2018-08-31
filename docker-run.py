import argparse
import subprocess
import os
import sys


def main():
    if len(sys.argv) == 2 and sys.argv[1] == "stop":
        docker_path = "/usr/bin/docker"

        container_ids = [
            cont_id
            for cont_id in (
                subprocess.check_output([
                    docker_path,
                    "ps", "--all", "-q", "--filter", "ancestor=distiller-worker"
                ]) +
                subprocess.check_output([
                    docker_path,
                    "ps", "--all", "-q", "--filter", "ancestor=distiller-daemon"
                ])
            ).decode("utf-8").split("\n")
            if cont_id != ""
        ]

        if len(container_ids) > 0:
            subprocess.call([
                docker_path,
                "stop"
            ] + container_ids)

            subprocess.call([
                docker_path,
                "rm"
            ] + container_ids)
        return

    parser = argparse.ArgumentParser(description="Start docker containers for Distiller daemon and workers")
    parser.add_argument(
        "tasks",
        help="Task definitions directory"
    )
    parser.add_argument(
        "data",
        help="Data storage directory"
    )
    parser.add_argument(
        "confs",
        nargs="+",
        help="""
        Configuration files to use
        (
            If one is specified: Take for all,
            If two are specified: Take first for daemon and second for workers
        )
        """
    )
    parser.add_argument("--num-workers", metavar="W", type=int, help="Number of workers to start", default=1)

    args = parser.parse_args()

    num_confs = len(args.confs)

    if num_confs < 1 or num_confs > 2:
        print("%i configurations specified; 1 or 2" % num_confs)
        parser.print_usage()
        return

    tasks_path = os.path.abspath(os.path.expanduser(args.tasks))
    data_path = os.path.abspath(os.path.expanduser(args.data))

    procs = []

    for i in range(0, args.num_workers + 1):
        conf_path = os.path.abspath(os.path.expanduser(args.confs[0 if i == 0 or num_confs == 1 else 1]))

        procs.append(subprocess.Popen([
            "docker", "run", "--rm",
            "-v", "%s:/conf.json" % conf_path,
            "-v", "%s:/tasks" % tasks_path,
            "-v", "%s:/data" % data_path,
            "-e", "DISTILLER_CONF_FILE=/conf.json",
            "-e", "DISTILLER_TASK_PATH=/tasks",
            "-e", "DISTILLER_DATA_PATH=/data",
            "distiller-%s:latest" % ("daemon" if i == 0 else "worker"),
        ]))
    
    for proc in procs:
        proc.wait()


if __name__ == "__main__":
    main()
