from unittest import TestCase, main
from stpstone.dsa.queues.simple_queue import Queue


class TestQueue(TestCase):
    def setUp(self):
        self.queue = Queue()

    def test_enqueue_and_dequeue(self):
        self.queue.enqueue(10)
        self.queue.enqueue(20)
        self.assertEqual(self.queue.dequeue, 10)
        self.assertEqual(self.queue.dequeue, 20)

    def test_is_empty(self):
        self.assertTrue(self.queue.is_empty)
        self.queue.enqueue(5)
        self.assertFalse(self.queue.is_empty)
        self.queue.dequeue
        self.assertTrue(self.queue.is_empty)

    def test_size(self):
        self.assertEqual(self.queue.size, 0)
        self.queue.enqueue(5)
        self.queue.enqueue(15)
        self.assertEqual(self.queue.size, 2)
        self.queue.dequeue
        self.assertEqual(self.queue.size, 1)

    def test_dequeue_empty_queue(self):
        with self.assertRaises(IndexError):
            self.queue.dequeue

if __name__ == "__main__":
    main()
