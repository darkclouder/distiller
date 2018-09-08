from distiller.api.AbstractTask import AbstractTask


class DefaultStill(AbstractTask):
    def stored_in(self):
        return None

    def requires(self):
        return []
