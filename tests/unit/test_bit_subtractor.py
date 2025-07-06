import unittest

from stpstone.analytics.arithmetic.bit_subtractor import FullSubtractor, HalfSubtractor


class TestHalfSubtractor(unittest.TestCase):
    """Test cases for HalfSubtractor class."""

    def test_normal_operations(self):
        """Test normal operations of HalfSubtractor."""
        # test all possible input combinations
        self.assertEqual(HalfSubtractor(0, 0).get_difference(), 0)
        self.assertEqual(HalfSubtractor(0, 0).get_borrow(), 0)

        self.assertEqual(HalfSubtractor(0, 1).get_difference(), 1)
        self.assertEqual(HalfSubtractor(0, 1).get_borrow(), 1)

        self.assertEqual(HalfSubtractor(1, 0).get_difference(), 1)
        self.assertEqual(HalfSubtractor(1, 0).get_borrow(), 0)

        self.assertEqual(HalfSubtractor(1, 1).get_difference(), 0)
        self.assertEqual(HalfSubtractor(1, 1).get_borrow(), 0)

    def test_type_validation(self):
        """Test type validation in HalfSubtractor."""
        # test invalid types
        with self.assertRaises(TypeError):
            HalfSubtractor("0", 1)
        with self.assertRaises(TypeError):
            HalfSubtractor(0, "1")
        with self.assertRaises(TypeError):
            HalfSubtractor(0.5, 1)
        with self.assertRaises(TypeError):
            HalfSubtractor(0, 1.0)

    def test_value_validation(self):
        """Test value validation in HalfSubtractor."""
        # test invalid values (non-binary)
        with self.assertRaises(ValueError):
            HalfSubtractor(2, 1)
        with self.assertRaises(ValueError):
            HalfSubtractor(0, -1)
        with self.assertRaises(ValueError):
            HalfSubtractor(3, 4)

    def test_docstring_examples(self):
        """Test the examples provided in docstrings."""
        subtractor = HalfSubtractor(1, 0)
        self.assertEqual(subtractor.get_difference(), 1)
        self.assertEqual(subtractor.get_borrow(), 0)

        self.assertEqual(HalfSubtractor(0, 1).get_borrow(), 1)
        self.assertEqual(HalfSubtractor(1, 0).get_borrow(), 0)


class TestFullSubtractor(unittest.TestCase):
    """Test cases for FullSubtractor class."""

    def test_normal_operations(self):
        """Test normal operations of FullSubtractor."""
        # test various combinations
        self.assertEqual(FullSubtractor(0, 0, 0).get_difference(), 0)
        self.assertEqual(FullSubtractor(0, 0, 0).get_borrow_out(), 0)

        self.assertEqual(FullSubtractor(0, 1, 0).get_difference(), 1)
        self.assertEqual(FullSubtractor(0, 1, 0).get_borrow_out(), 1)

        self.assertEqual(FullSubtractor(1, 0, 1).get_difference(), 0)
        self.assertEqual(FullSubtractor(1, 0, 1).get_borrow_out(), 0)

        self.assertEqual(FullSubtractor(1, 1, 1).get_difference(), 1)
        self.assertEqual(FullSubtractor(1, 1, 1).get_borrow_out(), 1)

    def test_borrow_propagation(self):
        """Test borrow propagation in FullSubtractor."""
        # test cases where borrow is generated
        self.assertEqual(FullSubtractor(0, 1, 0).get_borrow_out(), 1)
        self.assertEqual(FullSubtractor(0, 0, 1).get_borrow_out(), 1)
        self.assertEqual(FullSubtractor(1, 1, 1).get_borrow_out(), 1)

    def test_type_validation(self):
        """Test type validation in FullSubtractor."""
        # test invalid types
        with self.assertRaises(TypeError):
            FullSubtractor("0", 1, 0)
        with self.assertRaises(TypeError):
            FullSubtractor(0, [1], 0)
        with self.assertRaises(TypeError):
            FullSubtractor(0, 1, 0.5)

    def test_value_validation(self):
        """Test value validation in FullSubtractor."""
        # test invalid values (non-binary)
        with self.assertRaises(ValueError):
            FullSubtractor(2, 1, 0)
        with self.assertRaises(ValueError):
            FullSubtractor(0, -1, 0)
        with self.assertRaises(ValueError):
            FullSubtractor(0, 1, 2)

    def test_docstring_examples(self):
        """Test the examples provided in docstrings."""
        subtractor = FullSubtractor(1, 0, 1)
        self.assertEqual(subtractor.get_difference(), 0)
        self.assertEqual(subtractor.get_borrow_out(), 0)

        self.assertEqual(FullSubtractor(1, 0, 1).get_borrow_out(), 0)
        self.assertEqual(FullSubtractor(0, 0, 1).get_borrow_out(), 1)


if __name__ == '__main__':
    unittest.main()