Command-line interface
======================

TODO!!!!! CLI usage has been changed a lot, adapt whole documentation, not only here

*Note: all CLI commands have to be altered on some systems that run Python 2 when using* ``python``.
For those use ``python3`` instead.
Check ``python --version`` to ensure Python 3 is running.

Daemon
------
The daemon can be started using the cli with
``python cli.py daemon --start``

Worker
------
``python cli.py worker --start [--host HOST] [--port PORT]``
where host and port denote an alternative host and port for the daemon to locate.

Spirit scheduling
-----------------
``python cli.py spirit add action still_id [--host HOST] [--port PORT] [-p P] [-o O]``
where host and port denote an alternative host and port for the daemon to locate.

Actions are either ``add`` or ``remove`` to add or remove a spirit from the scheduler.

``still_id`` denotes the still ID on dot notation.
``P`` is a json-string of parameters.

Together with the ``still_id`` ``P`` forms the spirit id.

To remove a persistent spirit run
``python cli.py spirit remove path.to.still -p '{params...}' -o '{"persistent": true}'``

``O`` denotes a set of scheduling options.
Those do not affect the spirit ID.

Options are:
  * ``age_requirement`` - ``null`` (or don't set) to not have any age requirement
    (cache will be always used if the spirit has ever been executed), or a number in seconds.
    ``0`` will always be executed.
  * ``persistent`` - Marks a spirit as persistent. That rule will be kept in the Meta DB and persistent across daemon
    restarts
  * ``start_date`` - Datetime when to first start a reoccurring execution (timed scheduling)
  * ``end_date`` - Datetime when to stop a rule to be execution (timed scheduling)
  * ``reoccurring`` - Set true if still should be rerun when age requirement is about to not being met any more.
    This is always true if ``"persistent": true``
  * ``priority`` - Priority of execution, default: ``0``.
    A targets execution branch will always be executed before a branch of lower priority (if executable).
