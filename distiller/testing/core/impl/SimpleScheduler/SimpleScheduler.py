import unittest

from distiller.utils.Environment import Environment
from distiller.utils.Configuration import Configuration
from distiller.core.impl.SimpleScheduler.SimpleScheduler import SimpleScheduler
from distiller.core.interfaces.Scheduler import FinishState


class TestSimpleScheduler(unittest.TestCase):
    def setUp(self):
        self.env = Environment(Configuration.load(override={
            "log": {
                "verbose_level": "DEBUG",
                "log_level": "never",
                "exit_level": "never"
            },
            "meta": {
                "module": "distiller.core.impl.SQLiteMeta",
                "file_path": "unit_tests.db",
                "volatile": True
            }
        }))
        self.scheduler = SimpleScheduler(self.env)

        self.t1 = (
            "testing.parameter_requires",
            {"requires": []}
        )
        self.t2 = (
            "testing.parameter_requires",
            {"requires": [self.t1]}
        )

    def test_add_single(self):
        self.scheduler.add_target(self.t1)
        self.assertEqual(self.scheduler.run_next()["spirit_id"], self.t1)

    def test_add_with_dependency(self):
        self.scheduler.add_target(self.t2)
        self.assertEqual(self.scheduler.time_until_next(), 0)

        transaction = self.scheduler.run_next()
        self.assertEqual(transaction["spirit_id"], self.t1)
        self.assertEqual(self.scheduler.run_next(), None)
        self.scheduler.finish_spirit(transaction["transaction_id"])

        transaction = self.scheduler.run_next()
        self.assertEqual(transaction["spirit_id"], self.t2)
        self.assertEqual(self.scheduler.run_next(), None)
        self.scheduler.finish_spirit(transaction["transaction_id"])

        self.assertEqual(self.scheduler.run_next(), None)

    def test_add_after_dependency(self):
        self.scheduler.add_target(self.t1)
        self.scheduler.add_target(self.t2)
        self.assertEqual(self.scheduler.time_until_next(), 0)

        transaction = self.scheduler.run_next()
        self.assertEqual(transaction["spirit_id"], self.t1)
        self.assertEqual(self.scheduler.run_next(), None)
        self.scheduler.finish_spirit(transaction["transaction_id"])

        transaction = self.scheduler.run_next()
        self.assertEqual(transaction["spirit_id"], self.t2)
        self.assertEqual(self.scheduler.run_next(), None)
        self.scheduler.finish_spirit(transaction["transaction_id"])

        self.assertEqual(self.scheduler.run_next(), None)

    def test_error_execution(self):
        self.scheduler.add_target(self.t1)

        transaction = self.scheduler.run_next()
        self.assertEqual(transaction["spirit_id"], self.t1)
        self.assertEqual(self.scheduler.run_next(), None)
        self.scheduler.finish_spirit(transaction["transaction_id"], finish_state=FinishState.EXEC_ERROR)

        self.assertEqual(self.scheduler.run_next(), None)
        self.assertTrue(self.scheduler.graph.is_empty())

    # TODO test timed Schedules
    # TODO test persistent schedules


if __name__ == "__main__":
    unittest.main()
