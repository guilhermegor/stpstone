from unittest import TestCase, main
from stpstone.utils.parsers.numbers import NumHandler


class TestNumHandler(TestCase):
    def setUp(self):
        self.num_converter = NumHandler().transform_to_float
        self.test_cases = [
            # (input, precision=2 result, precision=6 result)
            ("3,132.45%", 31.32, 31.3245),
            ("3.134,4343424%", 31.34, 31.344343),
            ("314.124.434,3333555", 314124434.33, 314124434.333355),
            ("1.234,56", 1234.56, 1234.56),
            ("1,234.56", 1234.56, 1234.56),
            ("1.234.567,89", 1234567.89, 1234567.89),
            ("1,234,567.89", 1234567.89, 1234567.89),
            ("4.56 bp", 0.0, 0.000456),
            ("(-)912.412.911,231", -912412911.23, -912412911.231),
            ("text tried", "text tried", "text tried"),
            ("Example TEXT", "Example TEXT", "Example TEXT"),
            ("9888 Example", "9888 Example", "9888 Example"),
            (True, True, True),
            ("55,987,544", 55987544.0, 55987544.0),
            ("846.874.688", 846874688.0, 846874688.0),
            ("-0,10%", 0.0, -0.001),
            ("0,10%", 0.0, 0.001),
            ("(912.412.911,231)", -912412911.23, -912412911.231),
            ("(81,234,567.1324432)", -81234567.13, -81234567.132443),
            ("058,234,567.1324432", 58234567.13, 58234567.132443),
            ("-0845547,789789", -845547.79, -845547.789789),
            ("085.944,789789", 85944.79, 85944.789789),
        ]

    def test_transform_to_float_precision_2(self):
        for case in self.test_cases:
            input_val, expected_2, _ = case
            with self.subTest(input=input_val):
                result = self.num_converter(input_val, int_precision=2)
                self.assertEqual(result, expected_2)

    def test_transform_to_float_precision_6(self):
        for case in self.test_cases:
            input_val, _, expected_6 = case
            with self.subTest(input=input_val):
                result = self.num_converter(input_val, int_precision=6)
                self.assertEqual(result, expected_6)

if __name__ == '__main__':
    main()