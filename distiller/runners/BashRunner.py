from subprocess import Popen, PIPE
import os
import json
import tempfile
import shutil

from distiller.api.Runner import Runner


class BashRunner(Runner):
    def __init__(self, script_file, pipe_dependency=None, mode="replace"):
        self.pipe_dependency = pipe_dependency
        self.script_file = script_file
        self.mode = mode

    def run(self, task_dir, parameters, input_readers, output_writer):
        file_readers = [
            input_reader.blob().open()
            for i, input_reader in enumerate(input_readers)
            if i != self.pipe_dependency
        ]

        input_dir = tempfile.mkdtemp()
        input_file_paths = []

        for i, reader in enumerate(file_readers):
            file_path = os.path.join(input_dir, "in_%i" % i)
            input_file_paths.append(file_path)

            with open(file_path, "wb") as f:
                for chunk in reader:
                    f.write(chunk)

        script_path = os.path.join(task_dir, self.script_file)

        if self.mode == "replace":
            write_mode = output_writer.replace()
        elif self.mode == "append":
            write_mode = output_writer.append()
        else:
            raise ValueError("Invalid mode '%s'" % self.mode)

        error_msg = ""

        p = Popen(
            [script_path] + input_file_paths + [json.dumps(parameters)],
            stdout=PIPE,
            stdin=PIPE,
            stderr=PIPE
        )

        if self.pipe_dependency is not None:
            pipe_reader = input_readers[self.pipe_dependency].it().open()

            try:
                for row in pipe_reader:
                    p.stdin.write(row + b"\n")
                p.stdin.close()
            except BrokenPipeError:
                pass

        for reader in file_readers:
            reader.close()

        with write_mode as w:
            for line in iter(p.stdout.readline, b''):
                w.write(line)

            error_msg += p.stderr.read().decode('ascii')
            exit_code = p.wait()

            if exit_code == 0:
                w.commit()

        shutil.rmtree(input_dir)

        if exit_code != 0:
            raise ChildProcessError(error_msg)
