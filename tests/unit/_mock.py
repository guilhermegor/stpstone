from typing import Protocol
from unittest.mock import MagicMock

import pytest


class APIClient(Protocol):
    """Protocol defining the API client interface."""
    def get_value(self) -> int: 
        """Get a value from the API.

        Returns
        -------
        int
            The retrieved value
        """


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
        api_client : APIClient
            Client object with get_value() method

        Returns
        -------
        int
            Remote value plus 10
        """
        return api_client.get_value() + 10


class TestCalculator:
    """Test suite for Calculator class."""

    @pytest.fixture
    def calculator(self) -> Calculator:
        """Fixture providing Calculator instance for tests.
        
        Returns
        -------
        Calculator
            Fresh Calculator instance
        """
        return Calculator()

    def test_add(self, calculator: Calculator) -> None:
        """Test addition functionality.
        
        Parameters
        ----------
        calculator : Calculator
            Calculator instance from fixture
        """
        result = calculator.add(2, 3)
        assert result == 5

    def test_fetch_remote_value(self, calculator: Calculator) -> None:
        """Test remote value fetching.
        
        Parameters
        ----------
        calculator : Calculator
            Calculator instance from fixture
        """
        mock_api_client = MagicMock()
        mock_api_client.get_value.return_value = 5
        result = calculator.fetch_remote_value(mock_api_client)
        mock_api_client.get_value.assert_called_once()
        assert result == 15