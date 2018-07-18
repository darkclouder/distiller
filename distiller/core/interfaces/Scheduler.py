from enum import Enum


class FinishState(Enum):
    SUCCESS = 0
    TIMEOUT = 1
    EXEC_ERROR = 2
    UNIT_ERROR = 3
    WORKER_ERROR = 4
    LOAD_ERROR = 5
    EXEC_ABORT = 6


class Scheduler:
    def run_next(self):
        """Attemps to run one spirit that is in queue

        Note: This does not directly run the target but only return it.
        By calling this method the scheduler assumes that target to be running

        Returns a dictionary with transaction_id and spirit, or None if there is no next spirit
        """

        raise NotImplementedError

    def finish_spirit(self, transaction_id, finish_state=FinishState.SUCCESS, message=None):
        """Indicate a spirit as finished and remove it from the scheduler

        Arguments:
        transaction_id -- Transaction id

        Keyword Arguments
        finish_state -- State to finish with, if this is not FinishState.SUCCESS the cask date is not updated
                        and further branch execution aborted
        message -- Optional message (mainly for execution errors to write to log)
        """

        raise NotImplementedError

    def time_until_next(self):
        """Returns the duration in seconds until the next currently scheduled spirit should be executed.
        Returns 0 if there are active spirits in the scheduler
        Returns None if there is no spirit to run
        """

        raise NotImplementedError

    def add_target(self, target_spirit, options={}):
        """Adds a spirit for only one execution to the scheduler.
        This is usually done for manual execution.

        Arguments:
        target_spirit -- The target spirit that is to be created

        Keyword arguments:
        options -- Scheduler options to apply for this target.
        """

        raise NotImplementedError

    def remove_target(self, target_spirit, persistent=False):
        """Remove a target from the scheduler.
        This only works if the target is not yet running.
        A permanently scheduled target will be removed once and for all if persistent=True is set.

        Arguments:
        target_spirit -- The target spirit to remove
        """

        raise NotImplementedError


class Scheduler:
    def __init__(self, env):
        pass

    def run_next(self):
        """Attemps to run one spirit that is in queue

        Note: This does not directly run the target but only return it.
        By calling this method the scheduler assumes that target to be running

        Returns a target or None if there is none to run
        """

        raise NotImplementedError

    def time_until_next(self):
        """Returns the duration in seconds until the next scheduled (timed) target should be running.
        Returns None if there is no scheduled target.
        """

        raise NotImplementedError

    def add_target(self, target_spirit, age_requirement, options={}):
        """Adds a spirit permanently to the scheduler

        Arguments:
        target_spirit -- The target spirit that is to be created

        Keyword arguments:
        age_requirement -- An age requirement in seconds to fulfill
                            This should be greater than 0, and consinderably greater than the usual execution time to
                            compute this spirit.
        options -- Scheduler options to apply for this target.
        """

        raise NotImplementedError

    def add_target_once(self, target_spirit, age_requirement=0, options={}):
        """Adds a spirit for only one execution to the scheduler.
        This is usually done for manual execution.

        Arguments:
        target_spirit -- The target spirit that is to be created

        Keyword arguments:
        age_requirement -- An age requirement in seconds to fulfill (useful for delayed execution).
                            For instant manual execution this should be 0. Default: 0
        options -- Scheduler options to apply for this target.
        """

        raise NotImplementedError()

    def remove_target(self, target_spirit):
        """Remove a target from the scheduler"""

        raise NotImplementedError
