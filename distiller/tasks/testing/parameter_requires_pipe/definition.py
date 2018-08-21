from distiller.api.DefaultPipe import DefaultPipe


class Still(DefaultPipe):
    def __init__(self, parameters=None):
        super().__init__(parameters)

    def default_parameters(self):
        return {"requires": []}

    def requires(self):
        return self.parameters["requires"]
