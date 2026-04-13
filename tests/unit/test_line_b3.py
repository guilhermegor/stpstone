"""Unit tests for B3 LINE API client implementation.

This module contains comprehensive unit tests for the B3 LINE API client,
covering all classes and methods with normal operations, edge cases, error
conditions, and type validations. Tests use mocking to simulate API responses,
ensuring no real network calls are made.

References
----------
.. [1] http://www.b3.com.br/data/files/2E/95/28/F1/EBD17610515A8076AC094EA8/GUIDE-TO-LINE-5.0-API.pdf
.. [2] https://line.bvmfnet.com.br/#/endpoints
"""

import pytest

from stpstone.utils.providers.br.line_b3.connection_api import ConnectionApi


# --------------------------
# ConnectionApi Tests
# --------------------------
class TestConnectionApi:
    """Test cases for ConnectionApi class."""

    def test_init_empty_credentials(self) -> None:
        """Test initialization with empty credentials raises ValueError.

        Verifies that providing empty credentials raises a ValueError with the
        expected message.

        Returns
        -------
        None
        """
        with pytest.raises(ValueError, match="All credentials must be non-empty"):
            ConnectionApi("", "secret", "1234", "5678")

    def test_validate_url_invalid(self) -> None:
        """Test URL validation with invalid URL raises ValueError.

        Verifies that non-https or empty URLs raise ValueError with appropriate
        messages.

        Returns
        -------
        None
        """
        with pytest.raises(ValueError, match="URL must start with https://"):
            ConnectionApi("client", "secret", "1234", "5678", "http://invalid.com")
        with pytest.raises(ValueError, match="URL cannot be empty"):
            ConnectionApi("client", "secret", "1234", "5678", "")