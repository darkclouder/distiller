Command-line interface
======================

The command-line interface is hierarchical.
That means, for each command there can be a list of sub-commands.
E.g., the command ``python cli.py remote`` has the sub-commands ``spirit`` and ``cask``,
to perform operations on the remote daemon for spirits and casks respectively.
To see the first level of commands, enter ``python cli.py`` with ``-h`` or ``--help``.
The help commands are available for each level.
To find out which commands are available for remote control, enter ``python cli.py remote -h``.

Daemon
------
The daemon can be started using the cli with
``python cli.py daemon start``

Worker
------
``python cli.py worker start [--host HOST] [--port PORT]``
where host and port denote an alternative host and port for the daemon to locate.

Spirit scheduling
-----------------
``python cli.py remote spirit add [-h] [--host HOST] [--port PORT] [-p P] [-o O] [--ar AR] [--persistent] still_id``
where host and port denote an alternative host and port for the daemon to locate.
Enter ``python cli.py remote spirit add -h`` to check which default host and port are set.

Actions are either ``add`` or ``remove`` to add or remove a spirit from the scheduler.

``still_id`` denotes the still ID on dot notation.
``--p`` is a json-string of parameters.

Together with the ``still_id`` the parameters specified in ``--p`` form the spirit id.

To remove a persistent spirit run
``python cli.py spirit remove path.to.still -p '{params...}' --persistent``

``--persistent`` is a shortcut for ``-o '{"persistent": true}'``,
``--ar AR`` is a shortcut for ``-o '{"age_requirement": AR}'``.

``--o`` denotes a set of scheduling options.
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
