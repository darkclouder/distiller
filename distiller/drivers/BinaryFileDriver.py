import os
import shutil

from distiller.api.Reader import Reader, ReadIterator
from distiller.api.Writer import Writer, WriteModes, WriteAfterCommitException
from distiller.drivers.internal.FileDriver import FileDriver, get_temp_path


class BinaryFileDriver(FileDriver):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def read(self, spirit, config):
        return FileReader(self._get_data_path(spirit, config), **self.kwargs)

    def write(self, spirit, config):
        data_path = self._get_data_path(spirit, config, create_path=True)

        return BinaryWriteModes(data_path, **self.kwargs)


class FileReader(Reader):
    def __init__(self, file_path, **kwargs):
        self.file_path = file_path
        self.kwargs = kwargs

    def blob(self):
        return BlobIterator(
            self.file_path,
            mode="r" + ("b" if self.kwargs.get("binary", False) else ""),
            **self.kwargs
        )

    def it(self):
        return self.blob()


class BlobIterator(ReadIterator):
    def __init__(self, file_path, chunk_size=1024, mode="rb", **kwargs):
        self.file_path = file_path
        self.chunk_size = chunk_size
        self.file = None
        self.kwargs = kwargs
        self.mode = mode

    def __enter__(self):
        self.file = open(self.file_path, self.mode)

        return self

    def __exit__(self, type, value, traceback):
        if self.file is not None and not self.file.closed:
            self.file.close()

    def __iter__(self):
        if self.file is None:
            raise RuntimeError("BlobIterator must be entered with with-statement before usage")

        while True:
            chunk = self.file.read(self.chunk_size)

            if chunk:
                yield chunk
            else:
                break


class FileWriter(Writer):
    def __init__(self, file_path, mode, **kwargs):
        self.file_path = file_path
        self.kwargs = kwargs
        self.committed = False
        self.mode = mode + ("b" if self.kwargs.get("binary", False) else "")
        self.file = None

    def write(self, data):
        """Write a relational entry, or an entire blob"""

        if self.committed:
            raise WriteAfterCommitException

        self.file.write(data)

    def commit(self):
        """Commits the change. Write operations after this lead to an error"""

        if self.committed:
            raise WriteAfterCommitException

        self.committed = True
        self.file.close()
        self.file = None

        shutil.move(get_temp_path(self.file_path), self.file_path)

    def __enter__(self):
        self.file = open(get_temp_path(self.file_path), self.mode, **self.kwargs.get("file_params", {}))

        return self

    def __exit__(self, type, value, traceback):
        """If exit appears without a commit, undo all changes"""

        if not self.committed:
            self.file.close()
            os.remove(get_temp_path(self.file_path))
            self.file = None


class BinaryWriteModes(WriteModes):
    def __init__(self, file_path, **kwargs):
        self.kwargs = kwargs
        self.file_path = file_path

    def replace(self):
        return FileWriter(self.file_path, "w", **self.kwargs)

    def append(self):
        return FileWriter(self.file_path, "a", **self.kwargs)


module_class = BinaryFileDriver
