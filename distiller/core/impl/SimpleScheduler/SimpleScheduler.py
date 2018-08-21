from threading import Lock
import datetime

from distiller.core.interfaces.Scheduler import Scheduler, FinishState
from distiller.core.impl.SimpleScheduler.SchedulingGraph import SchedulingGraph
from distiller.core.impl.SimpleScheduler.SchedulingBacklog import SchedulingBacklog
from distiller.core.impl.SimpleScheduler.SchedulingInfo import SchedulingInfo

from distiller.api.AbstractTask import spirit_id_to_label

# TODO: handle pipes in Scheduler
# pipes are only executed when read from
# so simply remove them from the execution tree? (just skip them when exploring the execution tree, and using its
# dependencies as previous nodes instead)


class SimpleScheduler(Scheduler):
    def __init__(self, env):
        self.env = env
        self.logger = self.env.logger.claim("Scheduler")

        # Lock for controlling any scheduler access, since this can come from different threads
        self._lock = Lock()

        # Graph structure for active scheduler (tasks that are waiting for execution/are being executed)
        self.graph = SchedulingGraph(self.env, self.logger)

        # Backlog for scheduled tasks that are not yet actively needed
        self.backlog = SchedulingBacklog(self.env, self.logger)

    def run_next(self):
        with self._lock:
            # Add all targets from backlog that should be executed now to the active scheduled
            for schedule_info in self.backlog.consume_all():
                self.graph.add_target(schedule_info)

            # Get next (active) task to execute (depending on dependencies, priorities, etc)
            next_transaction = self.graph.run_next()

        if next_transaction is not None:
            self.logger.notice(
                "Start execution of %s (transaction id %i)" % (
                    spirit_id_to_label(*next_transaction["spirit_id"]), next_transaction["transaction_id"]
                )
            )

        return next_transaction

    def finish_spirit(self, transaction_id, finish_state=FinishState.SUCCESS, message=None):
        with self._lock:
            # Get spirit object for transaction_id
            spirit = self.graph.get_spirit(transaction_id)

            if finish_state == FinishState.SUCCESS:
                self.logger.notice(
                    "Successfully finished spirit %s (transaction id %i)" % (spirit, transaction_id)
                )
                self.graph.finish_spirit(transaction_id)

                # Only update cask date for successful execution
                self.env.meta.update_cask(spirit)
            else:
                error_message = "Spirit %s (transaction id %i) exited with state %s: %s" % (
                    spirit, transaction_id, finish_state.name, "No message" if message is None else message
                )

                if message:
                    self.logger.warning(error_message + ": %s" % message)
                else:
                    self.logger.warning(error_message)

                # Abort spirit (this also removes all tasks that depend on the errornous spirit)
                self.graph.abort_spirit(transaction_id)

    def time_until_next(self):
        with self._lock:
            if self.graph.is_empty():
                next_exec = self.backlog.next_execution()

                if next_exec is None:
                    return None

                return max(0, (next_exec - datetime.datetime.now()).total_seconds())

            return 0

    def add_target(self, target_spirit_id, options={}):
        schedule_info = SchedulingInfo(
            target_spirit_id,
            options.get("age_requirement", None),
            options.get("start_date", datetime.datetime.min),
            priority=options.get("priority", 0),
            reoccurring=options.get("reoccurring", False),
            start_date=options.get("start_date", None),
            end_date=options.get("end_date", None)
        )

        with self._lock:
            self.backlog.add(schedule_info, persistent=options.get("persistent", False))

        self.logger.notice("Add %s to scheduler with options %s" % (spirit_id_to_label(*target_spirit_id), options))

    def remove_target(self, target_spirit_id, persistent=False):
        with self._lock:
            self.backlog.remove(target_spirit_id, persistent)

    def event_still_updated(self, still):
        # TODO update scheduling graph with new still definition (update dependencies)
        pass

    def lock(self):
        return self._lock


module_class = SimpleScheduler
