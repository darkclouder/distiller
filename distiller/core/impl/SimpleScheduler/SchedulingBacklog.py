import heapq
import collections
import datetime

from .SchedulingInfo import SchedulingInfo
from distiller.utils.TaskLoader import TaskLoader, TaskLoadError


class SchedulingBacklog:
    def __init__(self, env, logger):
        self.env = env
        self.logger = logger
        self.backlog = []
        self.__load_persistent()

    def add(self, scheduling_info, persistent=False):
        """Add a scheduling rule to the backlog
        Only for once instance of the daemon if persistent=False
        Or in the meta db if persistent=True"""

        if persistent:
            scheduling_info.reoccurring = True
            scheduling_info = self.env.meta.add_scheduled_spirit(scheduling_info)

        heapq.heappush(self.backlog, scheduling_info)

    def next_execution(self):
        """Returns the datetime of the next execution.
        Note that this only checks for a possible execution to the time of the request.
        By adding, deletion or changed age requirements that time might be invalid and should be requested
        again if e.g. a sleep timer depends on it.
        Returns None if there is no item.
        """

        if len(self.backlog) > 0:
            return self.backlog[0].next_exec_date
        else:
            return None

    def consume_all(self):
        """Returns all scheduling infos which should be executed by now."""

        consumed = []

        now = datetime.datetime.now()

        while len(self.backlog) > 0 and self.backlog[0].next_exec_date <= now:
            head = heapq.heappop(self.backlog)

            if head.end_date is None or now <= head.end_date:
                assert head.start_date is None or head.start_date <= now

                pp_exec_date = self.__get_postponed_exec_date(head, now)

                if pp_exec_date is not None:
                    if pp_exec_date <= now:
                        consumed.append(head)
                        next_exec_date = self.__get_next_exec_date(head, now)

                        if next_exec_date is not None:
                            head.next_exec_date = next_exec_date
                            heapq.heappush(self.backlog, head)
                    else:
                        head.next_exec_date = pp_exec_date
                        heapq.heappush(self.backlog, head)

        return consumed

    def remove(self, spirit_id, persistent):
        """Remove spirit with spirit_id from backlog.
        If persistent=True remove persistent scheduled targets from meta db

        Arguments:
        spirit_id -- Spirit id of spirit to remove
        persistent -- Remove persistent targets completely?
        """

        self.backlog = [item for item in self.backlog if item.spirit_id != spirit_id]
        heapq.heapify(self.backlog)

        if persistent:
            self.env.meta.remove_scheduled_spirit(spirit_id)

    def __load_persistent(self):
        """Load all persistent scheduling rules from the meta db"""

        schedule_info = self.env.meta.get_scheduled_infos()

        for si in schedule_info:
            try:
                si.next_exec_date = self.__get_postponed_exec_date(si, datetime.datetime.now())
            except TaskLoadError as e:
                self.logger.warning(e)
            else:
                if si.next_exec_date is not None:
                    heapq.heappush(self.backlog, si)

    def __get_postponed_exec_date(self, scheduling_info, min_date):
        """Returns a new execution date for a scheduled target
        if it should be postponed.
        If the execution should happen right now returns date leq min_date.
        If there shouldn't be any execution at all return None

        Arguments:
        scheduling_info -- Scheduled target
        min_date -- As soon as the target or one of its dependencies does not satisfy this date return None
        """

        # Convert age requirement to timedelta (or None)
        if scheduling_info.age_requirement is None:
            age_td = None
        else:
            age_td = datetime.timedelta(seconds=scheduling_info.age_requirement)

        # Perform BFS on dependency tree starting with `scheduling_info`
        queue = collections.deque()
        spirit = TaskLoader.init(scheduling_info.spirit_id)

        queue.append(spirit)

        min_cask_dt = datetime.datetime.max

        while queue:
            curr = queue.popleft()

            cask_dt = self.__get_cask_datetime(curr)

            if cask_dt is None:
                # There is no cache -> execute
                return min_date

            if age_td is None:
                # Cask (cache) exists and there is no age requirement -> don't execute
                return None

            if cask_dt + age_td <= min_date:
                # Cask does not fulfil age requirement
                return min_date

            min_cask_dt = min(min_cask_dt, cask_dt)

            # Check age requirements for dependencies of curr
            # by adding it to the queue
            for dep in curr.requires():
                queue.append(TaskLoader.init(dep))

        pp_time = min_cask_dt + age_td - self.__predict_execution_time(spirit)

        # Don't execute at all if age requirements are already met and this is neither
        # reoccurring nor scheduled
        if pp_time > min_date and not scheduling_info.reoccurring and scheduling_info.start_date is None:
            return None

        return pp_time

    def __get_cask_datetime(self, target_spirit):
        cask_meta = self.env.meta.get_cask(target_spirit.spirit_id())

        if cask_meta is None:
            return None

        return cask_meta["last_completion"]

    def __get_next_exec_date(self, scheduling_info, from_date):
        """Returns a predicted next execution date for a scheduled target.
        Returns None if there shouldn't be a next execution.

        Note: This is only based on the from date and does not put the cask age of either
        the target nor of one of its dependencies into account.
        That means this method is only useful for scheduling the next exec date when
        it's directly being added to the active scheduler.

        Arguments:
        scheduling_info -- Scheduled target, should be reoccurring
        from_date -- Date to plan from
        """

        if not scheduling_info.reoccurring:
            return None

        if scheduling_info.age_requirement is None:
            return None

        exec_date = from_date + datetime.timedelta(seconds=scheduling_info.age_requirement)

        if scheduling_info.end_date is not None and exec_date > scheduling_info.end_date:
            return None

        return exec_date

    def __predict_execution_time(self, scheduling_info):
        """Returns a timedelta for the predicted execution time """
        # TODO (also do this together with backlog, not only graph)
        # i.e. graph has the same method, should be only implemented once somehow

        return datetime.timedelta(seconds=0)
