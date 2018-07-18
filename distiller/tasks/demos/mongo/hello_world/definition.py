from distiller.api.DefaultStill import DefaultStill
from distiller.runners.PythonRunner import PythonRunner
from distiller.drivers.MongoDriver import MongoDriver


# This class definition with its methods defines the still
# Each method should always return the same values for the
# same set of parameters.
# The return values should NOT depend on
# side effects (I/O, Date, Time, Randomness, ...)
# But ONLY on `self.parameters`
class Still(DefaultStill):
    def stored_in(self):
        return MongoDriver()

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
    with output_writer.append() as w:
        # Write all parameters
        w.write(parameters)

        # Commit changes
        # If this is not done:
        # All write operations are revoked
        # and the previous cached data remains
        w.commit()
