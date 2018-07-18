Create a runner
===============

A runner manages the input drivers and output driver and executes the actual job.
It is the runners responsibility to pass the input data to the job
and write the output data to the driver.

The interface to implement is:

.. autoclass:: distiller.api.Runner.Runner
  :members:


To look into an implementation see:

.. autoclass:: distiller.runners.PythonRunner.PythonRunner
