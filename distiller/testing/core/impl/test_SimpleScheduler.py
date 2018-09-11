import unittest
import datetime
import unittest.mock

from distiller.utils.Environment import Environment
from distiller.utils.Configuration import Configuration
from distiller.core.impl.SimpleScheduler.SimpleScheduler import SimpleScheduler
from distiller.core.interfaces.Scheduler import FinishState

# Source: http://blog.xelnor.net/python-mocking-datetime/
real_datetime_class = datetime.datetime


def mock_datetime_now(target, dt):
    class DatetimeSubclassMeta(type):
        @classmethod
        def __instancecheck__(mcs, obj):
            return isinstance(obj, real_datetime_class)

    class BaseMockedDatetime(real_datetime_class):
        @classmethod
        def now(cls, tz=None):
            return target.replace(tzinfo=tz)

        @classmethod
        def utcnow(cls):
            return target

    # Python2 & Python3 compatible metaclass
    MockedDatetime = DatetimeSubclassMeta('datetime', (BaseMockedDatetime,), {})

    return unittest.mock.patch.object(dt, 'datetime', MockedDatetime)


class TestSimpleScheduler(unittest.TestCase):
    def setUp(self):
        self.env = Environment(Configuration.load("daemon", override={
            "log": {
                "verbose_level": "DEBUG",
                "log_level": "never",
                "exit_level": "never"
            },
            "meta": {
                "module": "distiller.core.impl.SQLiteMeta",
                "file_path": "!:d/unit_tests.db",
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
        self.t3 = (
            "testing.parameter_requires_pipe",
            {"requires": [self.t2]}
        )
        self.t4 = (
            "testing.parameter_requires",
            {"requires": [self.t3]}
        )

    def test_add_single(self):
        self.scheduler.add_target(self.t1)
        self.assertEqual(self.scheduler.run_next()["spirit_id"], self.t1)

    def test_add_with_dependency(self):
        self.scheduler.add_target(self.t2)
        self.assertEqual(0, self.scheduler.time_until_next())

        transaction = self.scheduler.run_next()
        self.assertEqual(transaction["spirit_id"], self.t1)
        self.assertEqual(None, self.scheduler.run_next())
        self.scheduler.finish_spirit(transaction["transaction_id"])

        transaction = self.scheduler.run_next()
        self.assertEqual(transaction["spirit_id"], self.t2)
        self.assertEqual(None, self.scheduler.run_next())
        self.scheduler.finish_spirit(transaction["transaction_id"])

        self.assertEqual(None, self.scheduler.run_next())

    def test_add_after_dependency(self):
        self.scheduler.add_target(self.t1)
        self.scheduler.add_target(self.t2)

        self.assertEqual(self.scheduler.time_until_next(), 0)

        transaction = self.scheduler.run_next()
        self.assertEqual(transaction["spirit_id"], self.t1)
        self.assertEqual(None, self.scheduler.run_next())
        self.scheduler.finish_spirit(transaction["transaction_id"])

        transaction = self.scheduler.run_next()
        self.assertEqual(transaction["spirit_id"], self.t2)
        self.assertEqual(None, self.scheduler.run_next())
        self.scheduler.finish_spirit(transaction["transaction_id"])

        self.assertEqual(None, self.scheduler.run_next())

    def test_pipes(self):
        self.scheduler.add_target(self.t4)

        self.assertEqual(0, self.scheduler.time_until_next())

        transaction = self.scheduler.run_next()
        self.assertEqual(self.t1, transaction["spirit_id"])
        self.assertEqual(None, self.scheduler.run_next())
        self.scheduler.finish_spirit(transaction["transaction_id"])

        transaction = self.scheduler.run_next()
        self.assertEqual(self.t2, transaction["spirit_id"])
        self.assertEqual(None, self.scheduler.run_next())
        self.scheduler.finish_spirit(transaction["transaction_id"])

        transaction = self.scheduler.run_next()
        self.assertEqual(self.t4, transaction["spirit_id"])
        self.assertEqual(None, self.scheduler.run_next())
        self.scheduler.finish_spirit(transaction["transaction_id"])

        self.assertEqual(None, self.scheduler.run_next())

    def test_error_execution(self):
        self.scheduler.add_target(self.t1)

        transaction = self.scheduler.run_next()
        self.assertEqual(transaction["spirit_id"], self.t1)
        self.assertEqual(None, self.scheduler.run_next())
        self.scheduler.finish_spirit(transaction["transaction_id"], finish_state=FinishState.EXEC_ERROR)

        self.assertEqual(None, self.scheduler.run_next())
        self.assertTrue(self.scheduler.graph.is_empty())

    def test_multi_abort(self):
        # This method tests if an abort triggers only another abort being root at the same time,
        # or also one deeper in the exec tree

        self.scheduler.add_target(self.t2)
        transaction = self.scheduler.run_next()
        self.assertEqual(self.t1, transaction["spirit_id"])
        self.scheduler.finish_spirit(transaction["transaction_id"], finish_state=FinishState.SUCCESS)

        self.scheduler.add_target(self.t2, options={"age_requirement": 0})

        transaction = self.scheduler.run_next()
        self.assertEqual(self.t2, transaction["spirit_id"])
        self.scheduler.finish_spirit(transaction["transaction_id"], finish_state=FinishState.EXEC_ERROR)

        transaction = self.scheduler.run_next()
        self.assertEqual(self.t1, transaction["spirit_id"])

    def test_multi_abort2(self):
        self.scheduler.add_target(self.t2)
        self.scheduler.add_target(self.t2, options={"age_requirement": 0})

        transaction = self.scheduler.run_next()

        self.assertEqual(transaction["spirit_id"], self.t1)
        self.scheduler.finish_spirit(transaction["transaction_id"], finish_state=FinishState.EXEC_ERROR)

        self.assertEqual(None, self.scheduler.run_next())

    def test_postponing(self):
        # Test if postponing keeps target because at moment of start date it won't be satisfied any more

        now = datetime.datetime.now()

        self.scheduler.add_target(
            self.t1,
            options={"start_date": now + datetime.timedelta(seconds=10), "age_requirement": 5}
        )

        self.assertEqual(None, self.scheduler.run_next())

        self.scheduler.add_target(self.t1)

        transaction = self.scheduler.run_next()
        self.assertEqual(self.t1, transaction["spirit_id"])
        self.scheduler.finish_spirit(transaction["transaction_id"], finish_state=FinishState.SUCCESS)

        with mock_datetime_now(now + datetime.timedelta(seconds=11), datetime):
            self.assertEqual(self.t1, self.scheduler.run_next()["spirit_id"])

    def test_postponing_prune(self):
        # Test if postponing leads to pruning where requirements are satisfied

        now = datetime.datetime.now()

        self.scheduler.add_target(
            self.t1,
            options={"start_date": now + datetime.timedelta(seconds=10), "age_requirement": 15}
        )
        self.scheduler.add_target(self.t1)

        transaction = self.scheduler.run_next()
        self.assertEqual(self.t1, transaction["spirit_id"])
        self.scheduler.finish_spirit(transaction["transaction_id"], finish_state=FinishState.SUCCESS)

        with mock_datetime_now(now + datetime.timedelta(seconds=11), datetime):
            self.assertEqual(None, self.scheduler.run_next())



    # TODO test timed Schedules
    # TODO test persistent schedules


if __name__ == "__main__":
    unittest.main()
