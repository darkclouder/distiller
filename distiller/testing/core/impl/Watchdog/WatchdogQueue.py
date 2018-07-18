import unittest

from distiller.core.impl.Watchdog import WatchdogQueue

class WatchdogQueueTest(unittest.TestCase):
    def setUp(self):
        self.queue = WatchdogQueue()

    def test_operations(self):
        [self.queue.add(item) for item in [3, 4, 7, 5]]
        self.assertEqual(self.queue.keys(), [3, 4, 7, 5])

        self.queue.update(3)
        self.assertEqual(self.queue.keys(), [4, 7, 5, 3])

        self.queue.add(1)
        self.assertEqual(self.queue.keys(), [4, 7, 5, 3, 1])

        self.queue.update(4)
        self.assertEqual(self.queue.keys(), [7, 5, 3, 1, 4])

        self.queue.remove(3)
        self.assertEqual(self.queue.keys(), [7, 5, 1, 4])

        self.queue.remove(4)
        self.assertEqual(self.queue.keys(), [7, 5, 1])

        self.queue.remove(7)
        self.assertEqual(self.queue.keys(), [5, 1])

        self.queue.remove(5)
        self.assertEqual(self.queue.keys(), [1])

        self.queue.remove(1)
        self.assertEqual(self.queue.keys(), [])
