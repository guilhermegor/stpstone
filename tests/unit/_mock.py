import unittest
from unittest.mock import MagicMock


class Calculator:
    def add(self, a, b):
        return a + b

    def fetch_remote_value(self, api_client):
        return api_client.get_value() + 10


class TestCalculator(unittest.TestCase):

    def setUp(self):
        self.calc = Calculator()

    def test_add(self):
        result = self.calc.add(2, 3)
        self.assertEqual(result, 5)

    def test_fetch_remote_value(self):
        mock_api_client = MagicMock()
        mock_api_client.get_value.return_value = 5
        result = self.calc.fetch_remote_value(mock_api_client)
        mock_api_client.get_value.assert_called_once()
        self.assertEqual(result, 15)

if __name__ == '__main__':
    unittest.main()
