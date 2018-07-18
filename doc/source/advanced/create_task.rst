Create a task
=============

Tasks can be found in the configured task directory.
They can be accessed throughout the system when specifying dependencies of other tasks,
or adding a task to the scheduler with the CLI, through its dot notation.

The file structure looks like this:

::

  tasks/
    demos/
        open_street_maps/
          crawl/
            definition.py

The dot notation for this task is `demos.open_street_maps.crawl`

The `definition.py` specifies a task definition by defining a class inheriting AbstractTask.


Create a still
--------------

Stills are materialised tasks.
They are executed as an own entity and scheduled by the scheduler.

All stills are subclasses from

.. autoclass:: distiller.api.DefaultStill.DefaultStill

which inherits from AbstractTask.
All relevant methods that can be overridden to create a still can be seen there:

.. autoclass:: distiller.api.AbstractTask.AbstractTask
  :members:


Implement as many of those methods as you want to change the behaviour of your still
(add dependencies,
make a general still with multiple occurrences in execution branches,
custom runner,
etc.)
