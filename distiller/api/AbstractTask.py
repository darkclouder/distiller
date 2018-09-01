import json

from distiller.helpers.extend import extend
from distiller.api.DynamicClass import DynamicClass, class_id


def parameter_id(parameters):
    return json.dumps(parameters, sort_keys=True)


def spirit_id_to_label(still_id, parameters):
    return "%s(%s)" % (still_id, parameter_id(parameters))


@class_id("AbstractTask")
class AbstractTask(DynamicClass):
    def __init__(self, parameters=None):
        self.manual_parameters = parameters
        self.parameters = self.default_parameters()
        extend(self.parameters, self.manual_parameters)

    def default_parameters(self):
        """Returns a list of default parameters.
        Those values should be static to avoid side effects.
        """
        return {}

    def name(self):
        """Unique task identifier without parameters"""
        return self.__module__

    def label(self):
        """Unique task identifier label
        This should also include all parameters.
        Note: This should really be unique for a combination of task and parameters,
        since this is used for storing, hashing, __repr__, etc.
        This should always return the same value
        """

        return spirit_id_to_label(self.name(), self.parameters)

    def spirit_id(self):
        return self.name(), self.parameters

    def parameter_id(self):
        return parameter_id(self.parameters)

    def __repr__(self):
        return self.label()

    def __hash__(self):
        return hash(self.label())

    def __eq__(self, other):
        return self.name() == other.name() and self.parameters == other.parameters

    def requires(self):
        """Should return a list of tasks as dependencies, use [] or None for no dependency.
        This can also use the task parameters to return different dependencies, but should
        only use those to avoid side effects.
        """
        raise NotImplementedError

    def executed_by(self):
        """This should return the runner that executes the task.
        """
        raise NotImplementedError()

    def stored_in(self):
        """Should return a cask or None to determine where the task results are stored.
        If None is returned, this should be always returned, independent of parameters.
        If None is returned, this task is not materialised but instead executed every time.
        (Note: None is mainly used for non-materialised tasks such as pipes)
        """
        raise NotImplementedError

    def occurrences(self):
        """This returns an integer indicating how often a task can appear in an execution tree.
        This value exists to prevent endless recursive definitions.
        Note: A task with the exact same parameters can never reoccur,
        this is only for recurrences with different parameters.
        Usually this function should return 1.
        For special cases, like general tasks that are reused or pipes, this value might be `sys.maxsize`.
        Attention: Only return something different than 1 if you know what you're doing!
        """
        raise NotImplementedError

    def locks(self):
        """This returns a list of strings, each indicating an ID for a lock.
        A lock manages external dependencies (e.g. a crawler of a specific web site) by disallowing
        executions of spirits having a lock on the same ID.
        This can also use the task parameters to return different locks, but should
        only use those to avoid side effects.
        """
        return []
