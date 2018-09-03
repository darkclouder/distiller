import os
import tempfile
import tarfile
import dateutil.parser

from distiller.helpers.RequestHandler import RequestHandler
from distiller.utils.PathFinder import PathFinder
from distiller.utils.TaskLoader import TaskLoader
from distiller.utils.Configuration import Configuration
from distiller.core.interfaces.Scheduler import FinishState


class CoreHandler(RequestHandler):
    def __init__(self):
        super().__init__()

        self.get("/healthcheck", self.healthcheck)
        self.get("/tasks/definitions.tar.gz", self.get_tasks)
        self.get("/config/accumulated/worker.json", self.get_config("worker"))
        self.post("/tasks/run", self.run_next)
        self.post("/tasks/finish/(?P<transaction_id>[0-9]+)", self.finish)
        self.post("/tasks/heartbeat/(?P<transaction_id>[0-9]+)", self.heartbeat)
        self.post("/targets/add", self.add_target)
        self.post("/targets/remove", self.remove_target)
        self.post("/casks/remove/spirit", self.remove_cask_spirit)
        self.post("/casks/remove/(?P<mode>all|corrupt|unused)", self.remove_casks)

    def healthcheck(self, handle, params):
        handle.text(str(handle.server.env.distiller.is_running()))

    def get_tasks(self, handle, params):
        task_path = PathFinder.get_task_root()

        if os.path.exists(task_path) and os.path.isdir(task_path):
            _, tmp_file = tempfile.mkstemp()

            with tarfile.open(tmp_file, "w:gz") as tar:
                tar.add(task_path, arcname=".")

            handle.send_response(200)
            handle.send_header("content-type", "application/gzip")
            handle.end_headers()

            with open(tmp_file, "br") as f:
                handle.wfile.write(f.read())

            os.remove(tmp_file)
        else:
            handle.error(404)

    def get_config(self, mode):
        def get(handle, params):
            try:
                conf = Configuration.load(mode)
            except Exception as e:
                handle.server.env.logger.claim("CoreHandler").error(e)
                return handle.error(500)

            handle.json(conf.conf_dict)

        return get

    def run_next(self, handle, params, body):
        try:
            next_spirit = handle.server.env.scheduler.run_next()

            if next_spirit is not None:
                handle.server.env.watchdog.add(next_spirit["transaction_id"])
        except Exception as e:
            handle.server.env.logger.claim("CoreHandler").error(e)
            return handle.error(500)

        if next_spirit is None:
            time_until = handle.server.env.scheduler.time_until_next()

            handle.json({
                "wait_until": time_until
            })
        else:
            handle.json(next_spirit)

    def finish(self, handle, params, body):
        status = body.get("status", None)
        transaction_id = int(params["transaction_id"])
        message = body.get("message", None)

        try:
            finish_state = FinishState[status]
        except Exception:
            return handle.error(400)

        try:
            handle.server.env.watchdog.remove(transaction_id)
            handle.server.env.scheduler.finish_spirit(
                transaction_id,
                finish_state=finish_state,
                message=message
            )
        except ValueError | KeyError as e:
            return handle.json({
                "error": str(e)
            })
        except Exception as e:
            handle.server.env.logger.claim("CoreHandler").error(e)
            return handle.error(500)

        handle.json({"status": "ok"})

    def heartbeat(self, handle, params, body):
        transaction_id = int(params["transaction_id"])

        try:
            handle.server.env.watchdog.heartbeat(transaction_id)
            return handle.json({"status": "ok"})
        except ValueError as e:
            return handle.json({
                "error": str(e)
            })
        except Exception as e:
            handle.server.env.logger.claim("CoreHandler").error(e)
            return handle.error(500)

    def add_target(self, handle, params, body):
        try:
            spirit = body.get("spirit_id", None)
            options = body.get("options", {})

            if "start_date" in options:
                options["start_date"] = dateutil.parser.parse(options["start_date"])

            if "end_date" in options:
                options["end_date"] = dateutil.parser.parse(options["end_date"])
        except Exception as e:
            handle.server.env.logger.claim("CoreHandler").warning(e)
            return handle.error(400)

        if spirit is None:
            return handle.error(400)

        try:
            handle.server.env.scheduler.add_target(
                (spirit[0], spirit[1]),
                options=options
            )
        except Exception as e:
            handle.server.env.logger.claim("CoreHandler").error(e)
            return handle.error(500)

        handle.json({"status": "ok"})

    def remove_target(self, handle, params, body):
        try:
            spirit = body.get("spirit_id", None)
            options = body.get("options", {})
        except:
            return handle.error(400)

        if spirit is None:
            return handle.error(400)

        try:
            handle.server.env.scheduler.remove_target(
                (spirit[0], spirit[1]),
                persistent=options.get("persistent", False)
            )
        except Exception as e:
            handle.server.env.logger.claim("CoreHandler").error(e)
            return handle.error(500)

        handle.json({"status": "ok"})

    def remove_cask_spirit(self, handle, params, body):
        try:
            spirit_id = body.get("spirit_id", None)
        except Exception:
            return handle.error(400)

        if spirit_id is None:
            return handle.error(400)

        try:
            spirit = TaskLoader.init(spirit_id)
            handle.server.env.gc.delete_spirit(spirit)
        except Exception as e:
            handle.server.env.logger.claim("CoreHandler").error(e)
            return handle.error(500)

        handle.json({"status": "ok"})

    def remove_casks(self, handle, params, body):
        modes = {
            "all": handle.server.env.gc.delete_all,
            "corrupt": handle.server.env.gc.delete_corrupt,
            "unused": handle.server.env.gc.delete_unused,
        }

        mode = params["mode"]

        if mode not in modes:
            return handle.error(400)

        try:
            modes[mode]()
        except Exception as e:
            handle.server.env.logger.claim("CoreHandler").error(e)
            return handle.error(500)

        handle.json({"status": "ok"})
