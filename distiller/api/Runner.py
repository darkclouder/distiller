class Runner:
    def run(self, task_dir, parameters, input_readers, output_writer):
        """Start a (blocking) execution of a runner

        Arguments:
        task_dir -- Directory to the current task's files
        parameters -- Spirit parameters
        input_readers -- Array of reader object of dependencies specified in still's requires method
        output_writer -- Writer object of still's stored_in method, or default writer
        """

        raise NotImplementedError
