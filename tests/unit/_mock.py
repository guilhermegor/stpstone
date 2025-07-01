from typing import Protocol
import unittest
from unittest.mock import MagicMock


class APIClient(Protocol):
    def get_value(self) -> int: ...

class Calculator:
    """Simple calculator class for demonstration purposes."""
    
    def add(self, a: int, b: int) -> int:
        """Add two numbers together.

        Parameters
        ----------
        a : int
            First number
        b : int
            Second number

        Returns
        -------
        int
            Sum of a and b
        """
        return a + b

    def fetch_remote_value(self, api_client: APIClient) -> int:
        """Fetch and process a remote value.

        Parameters
        ----------
        api_client : Any
            Client object with get_value() method

        Returns
        -------
        int
            Remote value plus 10
        """
        return api_client.get_value() + 10


class TestCalculator(unittest.TestCase):
    
    def setUp(self) -> None:
        self.calc = Calculator()

    def test_add(self) -> None:
        result = self.calc.add(2, 3)
        self.assertEqual(result, 5)

    def test_fetch_remote_value(self) -> None:
        mock_api_client = MagicMock()
        mock_api_client.get_value.return_value = 5
        result = self.calc.fetch_remote_value(mock_api_client)
        mock_api_client.get_value.assert_called_once()
        self.assertEqual(result, 15)


if __name__ == '__main__':
    unittest.main()