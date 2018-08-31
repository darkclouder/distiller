from distiller.api.Reader import ReadIterator


class RowToBlobIterator(ReadIterator):
    def __init__(self, row_iterator):
        self.row_iterator = row_iterator

    def __enter__(self):
        self.it = self.row_iterator.__enter__()

        return self

    def __exit__(self, type, value, traceback):
        if self.it is not None:
            self.it.__exit__(type, value, traceback)

    def __iter__(self):
        if self.it is None:
            raise RuntimeError("Must be entered with with-statement before usage")

        for row in self.it:
            yield str(row) + "\n"
