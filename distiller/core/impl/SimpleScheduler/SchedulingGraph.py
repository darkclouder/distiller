import collections
import datetime
from enum import Enum

from distiller.utils.TaskLoader import TaskLoader
from distiller.utils.DependencyExplorer import DependencyExplorer


class SchedulingGraph:
    def __init__(self, env, logger):
        self.env = env
        self.logger = logger

        # Execution branches
        self.branches = []

        self.running_spirits = {}
        self.active_transactions = {}
        self.locks = {}

        self.next_transaction_id = 0

    def add_target(self, scheduling_info):
        """Add target with scheduling info to the active scheduling graph
        This creates a new execution branch.
        Dependencies are added until the age requirements are met.
        All tasks without any dependency any more will be considered as root
        and can be executed directly
        """

        now = datetime.datetime.now()

        # Build up complete execution tree
        # This has to be done to guarantee all age requirements
        # Otherwise it could be that for A -> B -> C the age requirement for B is met and for A is not
        # If I don't add B to the dependencies to be executed since its age requirement is met
        # then A and B will never be updated although A does not match the age requirement any more
        # Therefore the complete execution tree is built up first and then from top down
        # All roots are removed that fulfill the age requirements until none fulfills it

        roots = DependencyExplorer.explore(scheduling_info.spirit_id)

        # Remove cached results satisfying age requirements from execution tree
        fixed_roots = []

        while len(roots) > 0:
            # New roots in each reduction step
            # This has to be a set because maybe two children have both the same root as a
            # dependency, but it should only be considered once
            new_roots = set()

            for root in roots:
                cask_dt = self.__get_cask_datetime(root.spirit)

                if cask_dt is None or (
                    scheduling_info.age_requirement is not None and
                    cask_dt + datetime.timedelta(seconds=scheduling_info.age_requirement) < now
                ):
                    # Root does not satisfy age requirements, keep it
                    fixed_roots.append(root)
                else:
                    # Root satisfies age requirement, it can be removed from all children
                    # having this dependency
                    # And the children instead are now the next roots that would be executed next
                    # (if they don't meet the age requirement yet)

                    for child in root.children:
                        child.remove_parent(root)
                        new_roots.add(child)

            roots = list(new_roots)

        # Add (reduced) execution branch to list of branches
        self.branches.append(SchedulingBranch(scheduling_info, fixed_roots))

    def __predict_execution_time(self, spirit):
        """Returns a timedelta for the predicted execution time """
        # TODO (also do this together with backlog, not only graph)
        # i.e. backlog has the same method, should be only implemented once somehow

        return datetime.timedelta(seconds=0)

    def run_next(self):
        """Set the next target as running in graph"""
        spirit = self.__next()

        if spirit is not None:
            # Lock all locks of spirit
            self.__lock(spirit)

            # Assign transaction id
            transaction_id = self.next_transaction_id
            self.next_transaction_id += 1

            # Set spirit as running
            self.running_spirits[spirit] = transaction_id
            self.active_transactions[transaction_id] = spirit

            return {
                "transaction_id": transaction_id,
                "spirit_id": (spirit.name(), spirit.parameters)
            }

        return None

    def finish_spirit(self, transaction_id):
        """Mark a spirit's execution as finished and remove it from the scheduler"""
        spirit = self.__stop_spirit(transaction_id)

        new_branches = []

        # Remove spirit as roots from all branches in active scheduler to mark it as done
        # And to keep it from being executed by multiple branches if one execution is sufficient
        for branch in self.branches:
            if branch.contains_spirit(spirit):
                branch.finish_if_root(spirit)

                if not branch.is_complete():
                    new_branches.append(branch)
            else:
                new_branches.append(branch)

        self.branches = new_branches

    def is_empty(self):
        """Returns if the active scheduler is empty"""

        return len(self.branches) == 0

    def get_spirit(self, transaction_id):
        """Get spirit for a specific transaction_id"""

        if transaction_id not in self.active_transactions:
            raise KeyError("Invalid transaction id")

        return self.active_transactions[transaction_id]

    def abort_spirit(self, transaction_id):
        """Abort a running spirit and stop all of its ancestors"""

        spirit = self.__stop_spirit(transaction_id)
        self.branches = [branch for branch in self.branches if not branch.contains_spirit(spirit)]

    def __stop_spirit(self, transaction_id):
        if transaction_id not in self.active_transactions:
            raise ValueError("Invalid transaction id")

        spirit = self.active_transactions[transaction_id]

        assert spirit in self.running_spirits

        del self.active_transactions[transaction_id]
        del self.running_spirits[spirit]

        self.__unlock(spirit)

        return spirit

    def __get_cask_datetime(self, target_spirit):
        cask_meta = self.env.meta.get_cask(target_spirit)

        if cask_meta is None:
            return None

        return cask_meta["last_completion"]

    def __next(self):
        """Returns the next possible spirit to execute
        Returns None if there is no candidate"""

        self.branches = sorted(self.branches, key=lambda b: b.scheduling_info.priority, reverse=True)

        for branch in self.branches:
            for root in branch.roots:
                if root.spirit not in self.running_spirits and not self.__is_locked(root.spirit):
                    return root.spirit

        return None

    def __lock(self, spirit):
        for lock in spirit.locks():
            assert lock not in self.locks

            self.locks[lock] = True

    def __unlock(self, spirit):
        for lock in spirit.locks():
            self.locks.pop(lock)

    def __is_locked(self, spirit):
        for lock in spirit.locks():
            if lock in self.locks:
                return True

        return False


class SchedulingBranch:
    def __init__(self, scheduling_info, roots):
        self.scheduling_info = scheduling_info
        self.roots = roots

        self.spirits = {}

        # Set key map for all spirits being contained in this branch
        queue = collections.deque()
        queue.extend(roots)

        while queue:
            curr = queue.popleft()
            self.spirits[curr.spirit] = True

            for child in curr.children:
                queue.append(child)

    def finish_if_root(self, spirit):
        """Remove a spirit as done from execution branch.
        If the child of this spirit has no other dependencies any more
        make it root to mark it available next for execution
        """

        assert len(self.spirits) >= len(self.roots)

        new_roots = set()

        for root in self.roots:
            if root.spirit == spirit:
                # If dependency exists more than once it could happen
                # that it has been removed already
                # Therefore check if it exists
                if spirit in self.spirits:
                    self.spirits.pop(spirit)

                for child in root.children:
                    child.remove_parent(root)

                    if len(child.parents) == 0:
                        new_roots.add(child)
            else:
                new_roots.add(root)

        self.roots = list(new_roots)

    def is_complete(self):
        assert len(self.spirits) >= len(self.roots)

        return len(self.spirits) == 0

    def contains_spirit(self, spirit):
        return spirit in self.spirits
