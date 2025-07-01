from unittest import TestCase, main

from stpstone.dsa.stacks.simple_stack import Stack


class TestStack(TestCase):
    def setUp(self) -> None:
        self.stack = Stack()

    def test_push_and_pop(self) -> None:
        self.stack.push(1)
        self.stack.push(2)
        self.assertEqual(self.stack.pop(), 2)
        self.assertEqual(self.stack.pop(), 1)

    def test_is_empty(self) -> None:
        self.assertTrue(self.stack.is_empty)
        self.stack.push(10)
        self.assertFalse(self.stack.is_empty)
        self.stack.pop()
        self.assertTrue(self.stack.is_empty)

    def test_size(self) -> None:
        self.assertEqual(self.stack.size, 0)
        self.stack.push(100)
        self.assertEqual(self.stack.size, 1)
        self.stack.push(200)
        self.assertEqual(self.stack.size, 2)
        self.stack.pop()
        self.assertEqual(self.stack.size, 1)

    def test_peek(self) -> None:
        self.stack.push(5)
        self.assertEqual(self.stack.peek, 5)
        self.stack.push(10)
        self.assertEqual(self.stack.peek, 10)
        self.stack.pop()
        self.assertEqual(self.stack.peek, 5)

    def test_pop_empty_stack(self) -> None:
        with self.assertRaises(IndexError):
            self.stack.pop()

    def test_peek_empty_stack(self) -> None:
        with self.assertRaises(IndexError):
            assert self.stack.peek is not None

if __name__ == "__main__":
    main()
