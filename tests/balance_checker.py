from unittest import TestCase, main
from stpstone.transformations.validation.balance_brackets import BalanceBrackets


class TestBalanceChecker(TestCase):
    def setUp(self):
        self.checker = BalanceBrackets()

    def test_empty_string(self):
        self.assertTrue(self.checker.balance_checker(""))

    def test_balanced_parentheses(self):
        self.assertTrue(self.checker.balance_checker("()"))

    def test_balanced_brackets(self):
        self.assertTrue(self.checker.balance_checker("[]"))

    def test_balanced_braces(self):
        self.assertTrue(self.checker.balance_checker("{}"))

    def test_unbalanced_parentheses(self):
        self.assertFalse(self.checker.balance_checker("("))

    def test_unbalanced_brackets(self):
        self.assertFalse(self.checker.balance_checker("["))

    def test_unbalanced_braces(self):
        self.assertFalse(self.checker.balance_checker("{"))

    def test_mismatched_symbols(self):
        self.assertFalse(self.checker.balance_checker("(]"))

    def test_complex_balanced_string(self):
        self.assertTrue(self.checker.balance_checker("({[]})"))

    def test_complex_unbalanced_string(self):
        self.assertFalse(self.checker.balance_checker("({[}])"))

    def test_invalid_symbols(self):
        self.assertTrue(self.checker.balance_checker("abc"))

if __name__ == "__main__":
    main()
