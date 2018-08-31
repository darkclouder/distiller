import tempfile
import shutil
import importlib
import traceback
import threading
import time

from distiller.utils.Remote import Remote
from distiller.core.interfaces.Scheduler import FinishState
from distiller.utils.TaskLoader import TaskLoader, TaskLoadError
from distiller.utils.Configuration import Configuration
from distiller.utils.PathFinder import PathFinder


class Worker:
    def __init__(self, host, port):
        self.remote = Remote(host, port)

        # Load default driver
        self.config = Configuration.load("worker")

        driver_module = importlib.import_module(self.config.get("spirits.default_driver.module"))
        self.default_driver = driver_module.module_class(**self.config.get("spirits.default_driver.params"))

        self.task_dir = None

    def run_blocking(self):
        print("Worker running...")
        while True:
            job = self.__request_job()

            if job is not None:
                try:
                    self.__load_tasks()
                except Exception as e:
                    print(e)
                    self.__finish_job(job, FinishState.WORKER_ERROR, e)
                else:
                    self.__run_job(job)

                try:
                    shutil.rmtree(self.task_dir)
                except Exception as e:
                    print(e)
                finally:
                    self.task_dir = None
            else:
                # TODO: dynamic, wait_until
                time.sleep(5)

    def __request_job(self):
        try:
            res = self.remote.run_next()

            if res.get("transaction_id", None) is not None:
                return res

            # TODO: wait_until
        except Exception as e:
            print(e)

        return None

    def __load_tasks(self):
        # TODO: preserve downloaded tasks if there was no commit of new scripts
        self.task_dir = tempfile.mkdtemp()
        self.remote.download_tasks(self.task_dir)

    def __run_job(self, job):
        try:
            spirit = TaskLoader.init(job["spirit_id"], task_root=self.task_dir)
        except TaskLoadError:
            trace = traceback.format_exc()
            print("Could not load job %s" % job)
            print(trace)
            self.__finish_job(job, FinishState.LOAD_ERROR, message=trace)
        except Exception:
            trace = traceback.format_exc()
            print("Aborted job %s with error %s" % (job, trace))
            self.__finish_job(job, FinishState.WORKER_ERROR, message=trace)
        else:
            # Run spirit with runner, and send different finish states
            # depending on if there was an execution error, unit test error, abort, or success

            try:
                dep_input = self.__load_dependencies(spirit)

                if spirit.stored_in() is None:
                    writer = self.default_driver.write(spirit, self.config)
                else:
                    writer = spirit.stored_in().write(spirit, self.config)
            except Exception:
                trace = traceback.format_exc()
                print("Aborted spirit %s with error %s" % (spirit, trace))
                self.__finish_job(job, FinishState.LOAD_ERROR, message=trace)
            else:
                do_heartbeat = True

                try:
                    print("Run spirit %s" % spirit)

                    def do_thread():
                        timeout = 0.1 * self.config.get("watchdog.timeout")
                        transaction_id = job["transaction_id"]

                        while do_heartbeat:
                            self.remote.heartbeat(transaction_id)
                            time.sleep(timeout)

                    heartbeat_thread = threading.Thread(target=do_thread)
                    heartbeat_thread.start()

                    spirit.executed_by().run(
                        PathFinder.get_task_path(spirit.name(), task_root=self.task_dir),
                        spirit.parameters,
                        dep_input,
                        writer
                    )
                except Exception:
                    trace = traceback.format_exc()
                    print("Aborted spirit %s with error %s" % (spirit, trace))
                    self.__finish_job(job, FinishState.EXEC_ERROR, message=trace)
                else:
                    print("Completed spirit %s" % spirit)
                    self.__finish_job(job, FinishState.SUCCESS)

                do_heartbeat = False

    def __load_dependencies(self, spirit):
        deps = [TaskLoader.init(dep) for dep in spirit.requires()]

        return [
            self.default_driver.read(dep, self.config)
            if dep.stored_in() is None
            else dep.stored_in().read(dep, self.config)
            for dep in deps
        ]

    def __finish_job(self, job, finish_state, message=None):
        self.remote.finish_task(job["transaction_id"], finish_state, message)
