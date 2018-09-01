BashRunner
==========

.. autoclass:: distiller.runners.BashRunner.BashRunner

The bash runner simply runs a script file.

Returning a status code != 0 will abort the task.
The content of stderr will be sent
to the daemon for debugging/logging purposes.

All input dependencies are (except for ``pipe_dependency``) written to a temporary file
before script execution and passed as command-line arguments, together with a json-string of
all parameters (in that order).

The pipe dependency is piped row-based via stdin.

Stdout will be written to the output writer.

Configuration
-------------

The constructor of ``BashRunner`` takes one obligatory argument, the relative path
(within the directory of ``definition.py``) of the script file to run.

``pipe_dependency`` can be set to a number representing the index of the input dependencies
which should be piped rather than passed as file.

``mode`` (default: ``replace``) can be either ``replace`` or ``append``, and specifies
with which writing mode to write to the output. Note: ``update`` is not supported.

Examples
--------

Example still that simply pipes the input of ``demos.bash.hello_world`` to the
stdin of ``run.sh``, which then simply writes it to stdout.
This basically copies the input of ``demos.bash.hello_world`` to a file.

::

  from distiller.api.DefaultStill import DefaultStill
  from distiller.runners.BashRunner import BashRunner
  from distiller.drivers.BinaryFileDriver import BinaryFileDriver


  class Still(DefaultStill):
      def stored_in(self):
          return BinaryFileDriver(binary=True)

      def requires(self):
          return [
              ("demos.bash.hello_world", {})
          ]

      def executed_by(self):
          return BashRunner("run.sh", pipe_dependency=0)


``run.sh``:

::

  #!/bin/bash

  read pipe_input
  echo $pipe_input
