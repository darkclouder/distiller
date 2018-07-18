import unittest

from distiller.utils.TaskLoader import TaskLoader, TaskLoadError

class TestTaskLoader(unittest.TestCase):
    def setUp(self):
        pass

    def test_not_found(self):
        with self.assertRaises(TaskLoadError):
            TaskLoader.load("testing.not_existing")

    def test_missing_definition_file(self):
        with self.assertRaises(TaskLoadError):
            TaskLoader.load("testing.missing_definition_file")

    def test_missing_definition(self):
        with self.assertRaises(TaskLoadError):
            TaskLoader.load("testing.missing_definition")

    def test_corrupt_definition(self):
        with self.assertRaises(TaskLoadError):
            TaskLoader.load("testing.corrupt_definition")

    def test_valid_definition(self):
        ValidSpirit = TaskLoader.load("testing.valid_definition")
        aSpirit = ValidSpirit()
        self.assertTrue(aSpirit.a_test_function())

if __name__ == "__main__":
    unittest.main()
