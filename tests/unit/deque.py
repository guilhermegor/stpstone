from stpstone.dsa.queues.simple_deque import Deque
from unittest import TestCase, main


class TestDeque(TestCase):
    def setUp(self) -> None:
        self.deque = Deque()

    def test_is_empty_initially(self):
        self.assertTrue(self.deque.is_empty)

    def test_add_front(self):
        self.deque.add_front(10)
        self.assertFalse(self.deque.is_empty)
        self.assertEqual(self.deque.size, 1)
        self.assertEqual(self.deque.remove_front, 10)

    def test_add_rear(self):
        self.deque.add_rear(20)
        self.assertFalse(self.deque.is_empty)
        self.assertEqual(self.deque.size, 1)
        self.assertEqual(self.deque.remove_rear, 20)

    def test_remove_front(self):
        self.deque.add_front(30)
        self.deque.add_front(40)
        self.assertEqual(self.deque.remove_front, 40)
        self.assertEqual(self.deque.size, 1)

    def test_remove_rear(self):
        self.deque.add_rear(50)
        self.deque.add_rear(60)
        self.assertEqual(self.deque.remove_rear, 60)
        self.assertEqual(self.deque.size, 1)

    def test_mixed_operations(self):
        self.deque.add_front(1)
        self.deque.add_rear(2)
        self.deque.add_front(3)
        self.assertEqual(self.deque.size, 3)
        self.assertEqual(self.deque.remove_rear, 2)
        self.assertEqual(self.deque.remove_front, 3)
        self.assertEqual(self.deque.remove_front, 1)
        self.assertTrue(self.deque.is_empty)


if __name__ == "__main__":
    main()
