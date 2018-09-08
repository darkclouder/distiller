from distiller.api.DefaultStill import DefaultStill


class Still(DefaultStill):
    def default_parameters(self):
        return {"n": 1, "m": 1}

    def requires(self):
        if self.parameters["n"] > 0:
            return [("testing.recursive_dependency", {"n": self.parameters["n"] - 1, "m": self.parameters["m"] - 1})]
        else:
            return []

    def occurrences(self):
        return self.parameters["m"]