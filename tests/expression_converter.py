from unittest import TestCase, main
from stpstone.utils.conversions.expression_converter import ExpressionConverter


class TestExpressionConverter(TestCase):

    def test_infix_to_postfix(self):
        expr_ = "A + B * C - D / E"
        converter = ExpressionConverter(str_expr=expr_, str_from_type="infix", str_to_type="postfix")
        expected_postfix_expr = "A B C * + D E / -"
        result = converter.convert
        self.assertEqual(result, expected_postfix_expr)

    def test_infix_to_prefix(self):
        expr_ = "A + B * C - D / E"
        converter = ExpressionConverter(str_expr=expr_, str_from_type="infix", str_to_type="prefix")
        expected_expr = "- + A * B C / D E"
        result = converter.convert
        self.assertEqual(result, expected_expr)

    def test_postfix_to_infix(self):
        expr_ = "A B C * + D E / -"
        converter = ExpressionConverter(str_expr=expr_, str_from_type="postfix", str_to_type="infix")
        expected_expr = "((A + (B * C)) - (D / E))"
        result = converter.convert
        self.assertEqual(result, expected_expr)

    def test_postfix_to_prefix(self):
        expr_ = "A B C * + D E / -"
        converter = ExpressionConverter(str_expr=expr_, str_from_type="postfix", str_to_type="prefix")
        expected_expr = "- + A * B C / D E"
        result = converter.convert
        self.assertEqual(result, expected_expr)

    def test_prefix_to_infix(self):
        expr_ = "- + A * B C / D E"
        converter = ExpressionConverter(str_expr=expr_, str_from_type="prefix", str_to_type="infix")
        expected_expr = "((A + (B * C)) - (D / E))"
        result = converter.convert
        self.assertEqual(result, expected_expr)

    def test_prefix_to_postfix(self):
        expr_ = "- + A * B C / D E"
        converter = ExpressionConverter(str_expr=expr_, str_from_type="prefix", str_to_type="postfix")
        expected_expr = "A B C * + D E / -"
        result = converter.convert
        self.assertEqual(result, expected_expr)

if __name__ == "__main__":
    main()
