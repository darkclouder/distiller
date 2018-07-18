Quickstart
==========

Terminology
-----------
* Task: child class of ``distiller.api.AbstractTask``
* Still: child class of ``distiller.api.DefaultStill``, a materalised task
* Still ID/Task ID: Unique identifier string, path to task folder in dot notation
* Spirit: Instance of a still class
* Spirit ID: Unique identifier tuple (still_id, parameters) of a spirit


Start the system
----------------
The system can be installed and configured on multiple systems.
Daemon and worker can be running on the same host, or different machines.
If worker runs on different machines either set the configuration values for host and port in the configuration file
to the host and port of the daemon process/machine, or specify it when starting the worker.

To start the daemon run ``python cli.py daemon --start``.

`Note: On some systems python refers to Python 2. Use` ``python3 cli.py`` `instead.`

To start the worker run ``python cli.py worker --start [--host HOST] [--port PORT]`` where host and port refer
to host and port of the daemon process/machine.

`Note: If you want to run single machine setup but still have parallel execution start multiple worker processes.
Each worker process can only execute one spirit at a time.`


Create a simple still
---------------------

To create a simple still create a file ``demos/quickstart/hello_world/definition.py``:

::

  from distiller.api.DefaultStill import DefaultStill
  from distiller.runners.PythonRunner import PythonRunner

  # This class definition with its methods defines the still
  # Each method should always return the same values for the
  # same set of parameters.
  # The return values should NOT depend on
  # side effects (I/O, Date, Time, Randomness, ...)
  # But ONLY on `self.parameters`
  class Still(DefaultStill):
    def executed_by(self):
        # Define who executes what
        # A python function `do` is executed
        return PythonRunner(do)

    def default_parameters(self):
        # Set default values for all parameters
        return {"text": "Hello World!"}

  def do(parameters, input_readers, output_writer):
    # Open the writer with
    # replace strategy (override all existing data)
    with output_writer.replace() as w:
      # Write parameter text
      w.write([parameters["text"]])
      # Commit changes
      # If this is not done:
      # All write operations are revoked
      # and the previous cached data remains
      w.commit()


As a final step transfer ``demos/quickstart/hello_world`` to the
(if not local: daemon's machine's) task folder, by default ``distiller/tasks``.

*Note: since there is no deployment system in place yet with automatic script updates across the daemon and workers,
each time an existing script is altered, workers and daemons have to be restarted.
This does not apply to new tasks. They can be added without restarting.*

**See** ``distiller/tasks/demos`` **for more still examples.**

Schedule your still
-------------------
To schedule the created still run
``python cli.py spirit add demos.quickstart.hello_world -p '{"text": "Hi!"}' [--host HOST] [--port PORT]``
on your workstation. This one will write ``Hi!`` to your default data driver, default: a file located at
``data/demos/quickstart/hello_world.``

If you want to alter the hello world still, restart the daemon and worker before execution and run
``python cli.py spirit add demos.quickstart.hello_world -p '{"text": "Hi!"}' -o '{"age_requirement": 0}' [--host HOST] [--port PORT]``

``-o`` specifies scheduler options, ``"age_requirement": 0`` means:
data has to be at most 0 seconds old, which enforces a re-execution every time.
