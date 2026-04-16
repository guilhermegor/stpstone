"""Unit tests for WebhookTeams class.

Tests the Microsoft Teams webhook messaging functionality including:
- URL validation
- Message sending with various inputs
- Error conditions and edge cases
"""

from unittest.mock import MagicMock, patch

import pymsteams
import pytest

from stpstone.utils.webhooks.teams import WebhookTeams


class TestWebhookTeams:
	"""Test cases for WebhookTeams messaging functionality."""

	@pytest.fixture
	def valid_webhook_url(self) -> str:
		"""Fixture providing a valid webhook URL.

		Returns
		-------
		str
			Valid Microsoft Teams webhook URL
		"""
		return "https://example.webhook.office.com/webhookb2/123456"

	@pytest.fixture
	def webhook_instance(self, valid_webhook_url: str) -> pymsteams.connectorcard:
		"""Fixture providing initialized WebhookTeams instance.

		Parameters
		----------
		valid_webhook_url : str
			Valid webhook URL from fixture

		Returns
		-------
		pymsteams.connectorcard
			Initialized WebhookTeams instance
		"""
		return pymsteams.connectorcard(valid_webhook_url)

	@pytest.fixture
	def sample_message(self) -> str:
		"""Fixture providing sample message text.

		Returns
		-------
		str
			Test message content
		"""
		return "Test message content"

	def test_init_with_valid_url(self, valid_webhook_url: str) -> None:
		"""Test initialization with valid URL.

		Verifies
		--------
		- Instance is created successfully with valid URL
		- URL is stored correctly in instance attribute

		Parameters
		----------
		valid_webhook_url : str
			Valid webhook URL from fixture
		"""
		teams = WebhookTeams(valid_webhook_url)
		assert teams.url_webhook == valid_webhook_url

	@pytest.mark.parametrize(
		"invalid_url,type_error",
		[
			("", ValueError),
			(None, TypeError),
			(123, TypeError),
			("http://insecure.url", ValueError),
			("ftp://wrong.protocol", ValueError),
			("justastring", ValueError),
		],
	)
	def test_init_with_invalid_url(self, invalid_url: str, type_error: type) -> None:
		"""Test initialization with invalid URLs.

		Verifies
		--------
		- ValueError is raised for invalid URL formats
		- Appropriate error message is included

		Parameters
		----------
		invalid_url : str
			Invalid URL to test
		type_error : type
			Expected exception type
		"""
		with pytest.raises(type_error):
			WebhookTeams(invalid_url)

	@pytest.mark.parametrize(
		"invalid_message,type_error",
		[("", ValueError), (None, TypeError), (123, TypeError), ([], TypeError), ({}, TypeError)],
	)
	def test_send_invalid_message(
		self, valid_webhook_url: str, invalid_message: str, type_error: type
	) -> None:
		"""Test sending invalid messages.

		Verifies
		--------
		- ValueError is raised for invalid message content
		- Appropriate error message is included

		Parameters
		----------
		valid_webhook_url : str
			Valid webhook URL from fixture
		invalid_message : str
			Invalid message content to test
		type_error : type
			Expected exception type
		"""
		teams = WebhookTeams(valid_webhook_url)
		with pytest.raises(type_error):
			teams.send_message(invalid_message)

	def test_send_message_failure(self, valid_webhook_url: str, sample_message: str) -> None:
		"""Test message sending failure.

		Verifies
		--------
		- ValueError is raised when sending fails
		- Original exception is chained
		- Error message includes original error

		Parameters
		----------
		valid_webhook_url : str
			Valid webhook URL from fixture
		sample_message : str
			Test message content from fixture
		"""
		mock_card = MagicMock()
		mock_card.send.side_effect = Exception("Network error")

		with patch("pymsteams.connectorcard", return_value=mock_card):
			teams = WebhookTeams(valid_webhook_url)
			with pytest.raises(ValueError) as excinfo:
				teams.send_message(sample_message)

			assert "Failed to send Teams message" in str(excinfo.value)
			assert "Network error" in str(excinfo.value.__cause__)
