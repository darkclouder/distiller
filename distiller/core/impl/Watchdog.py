import threading
import time
from datetime import datetime, timedelta

from ..interfaces.Scheduler import FinishState


class Watchdog:
    def __init__(self, env):
        self.env = env
        self.logger = self.env.logger.claim("Watchdog")
        self.running = False
        self.lock = threading.Lock()
        self.queue = WatchdogQueue()

    def run(self):
        if self.running:
            raise RuntimeWarning("Watchdog already running")

        self.logger.notice("Watchdog start-up")

        self.running = True
        srv_thread = threading.Thread(target=self.__run_thread)
        srv_thread.start()

    def stop(self):
        if not self.running:
            raise RuntimeWarning("Watchdog not running")

        self.logger.notice("Watchdog shutdown initiated")

        self.running = False

    def add(self, transaction_id):
        with self.lock:
            self.queue.add(transaction_id)

    def heartbeat(self, transaction_id):
        with self.lock:
            self.queue.update(transaction_id)

    def remove(self, transaction_id):
        with self.lock:
            self.queue.remove(transaction_id)

    def __run_thread(self):
        timeout = timedelta(seconds=self.env.config.get("watchdog.timeout"))

        while self.running:
            with self.lock:
                now = datetime.now()
                next_timeout = None
                remove_ids = []

                for item in self.queue.it():
                    if now - item.value > timeout:
                        remove_ids.append(item.key)
                        self.env.scheduler.finish_spirit(item.key, finish_state=FinishState.TIMEOUT)
                    else:
                        next_timeout = item.value
                        break

            # This is done after the loop of the queue to not interfere with the iteration
            for transaction_id in remove_ids:
                self.remove(transaction_id)

            # TODO: interrupt this sleep when stopped running somehow (maybe just kill thread)
            if next_timeout is None:
                time.sleep(timeout.total_seconds())
            else:
                sec_until_timeout = (next_timeout + timeout - now).total_seconds()
                time.sleep(max(0.0, sec_until_timeout))

        self.logger.notice("Watchdog shutdown done")


class WatchdogQueue:
    def __init__(self):
        self.head = self.tail = None
        self.key_map = {}

    def add(self, key):
        if key in self.key_map:
            raise KeyError("Transaction id already present")

        item = QueueItem(key, datetime.now())

        self.key_map[key] = item

        if self.head is None:
            self.head = self.tail = item
        else:
            self.tail.next = item
            item.prev = self.tail
            self.tail = item

    def update(self, key):
        self.remove(key)
        self.add(key)

    def remove(self, key):
        if key not in self.key_map:
            raise KeyError("Invalid transaction id")

        item = self.key_map[key]

        del self.key_map[key]

        if self.head == self.tail:
            self.head = self.tail = None
        else:
            if item == self.head:
                self.head = item.next
                self.head.prev = None
            elif item == self.tail:
                self.tail = item.prev
                self.tail.next = None
            else:
                item.prev.next = item.next
                item.next.prev = item.prev

    def keys(self):
        return [item.key for item in self.it()]

    def it(self):
        curr = self.head

        while curr is not None:
            yield curr
            curr = curr.next


class QueueItem:
    def __init__(self, key, value):
        self.key = key
        self.value = value
        self.next = None
        self.prev = None
