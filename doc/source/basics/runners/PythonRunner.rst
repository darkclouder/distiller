PythonRunner
============

.. autoclass:: distiller.runners.PythonRunner.PythonRunner

The python runner simply executes a python function.
Raising an exception, like in all runners in the runner function,
causes the task to be aborted and its exception message will be sent
to the daemon for debugging/logging purposes.

Configuration
-------------

The constructor of ``PythonRunner`` takes one obligatory argument, the function to call
for running as a positional argument.

The function to call should take three positional arguments, ``parameters, input_readers, output_writer``.

``parameters`` are the spirit parameters (dictionary),
``input_readers`` is an array of ``Reader`` in the order of ``requires()``,
``output_writer`` is an instance of ``WriteModes``.

Examples
--------

::

  def executed_by(self):
        return PythonRunner(do)

And ``do``:

::

  def do(parameters, input_readers, output_writer):
      with output_writer.replace() as writer:
          writer.write("Hello world")
          writer.commit()
