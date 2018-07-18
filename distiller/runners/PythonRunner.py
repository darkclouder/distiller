from distiller.api.Runner import Runner


class PythonRunner(Runner):
    def __init__(self, func):
        self.func = func

    def run(self, task_dir, parameters, input_readers, output_writer):
        self.func(parameters, input_readers, output_writer)
