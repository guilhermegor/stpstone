from unittest import TestCase, main
from stpstone.utils.conversions.base_converter import BaseConverter


class TestBaseConverter(TestCase):

    def test_conversion_valid_cases(self):
        converter = BaseConverter("25", 10, 2)
        self.assertEqual(converter.convert, "11001")
        converter = BaseConverter("25", 10, 16)
        self.assertEqual(converter.convert, "19")
        converter = BaseConverter("25", 10, 8)
        self.assertEqual(converter.convert, "31")
        converter = BaseConverter("11001", 2, 10)
        self.assertEqual(converter.convert, "25")
        converter = BaseConverter("19", 16, 10)
        self.assertEqual(converter.convert, "25")
        converter = BaseConverter("31", 8, 10)
        self.assertEqual(converter.convert, "25")

    def test_conversion_edge_cases(self):
        converter = BaseConverter("0", 2, 10)
        self.assertEqual(converter.convert, "0")
        converter = BaseConverter("1", 2, 10)
        self.assertEqual(converter.convert, "1")
        converter = BaseConverter("A", 16, 10)
        self.assertEqual(converter.convert, "10")
        with self.assertRaises(ValueError):
            BaseConverter("25", 1, 10)
        with self.assertRaises(ValueError):
            BaseConverter("25", 17, 10)

    def test_large_number(self):
        converter = BaseConverter("123456789", 10, 16)
        self.assertEqual(converter.convert, "75BCD15")
        converter = BaseConverter("75BCD15", 16, 10)
        self.assertEqual(converter.convert, "123456789")
        converter = BaseConverter("123456789", 10, 16)
        self.assertEqual(converter.convert, "75BCD15")
        converter = BaseConverter("75bcd15", 16, 10)
        self.assertEqual(converter.convert, "123456789")

    def test_invalid_characters(self):
        with self.assertRaises(ValueError):
            BaseConverter("z", 16, 10)
        with self.assertRaises(ValueError):
            BaseConverter("1z", 16, 10)
        with self.assertRaises(ValueError):
            BaseConverter("XUbADdsa12z", 16, 10)

if __name__ == "__main__":
    main()
