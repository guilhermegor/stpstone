"""Unit tests for WebhookSlack class.

Tests the Slack webhook message sending functionality including:
- Initialization with valid and invalid inputs
- Message sending operations
- Validation of URLs and channel IDs
- Error conditions and edge cases
"""

from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from requests.exceptions import HTTPError

from stpstone.utils.webhooks.slack import WebhookSlack


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def valid_webhook_url() -> str:
	"""Fixture providing a valid Slack webhook URL.

	Returns
	-------
	str
		Valid Slack webhook URL
	"""
	return "https://hooks.slack.com/services/T123/B456/A789"


@pytest.fixture
def valid_channel_id() -> str:
	"""Fixture providing a valid Slack channel ID.

	Returns
	-------
	str
		Valid channel ID starting with #
	"""
	return "#general"


@pytest.fixture
def webhook_slack_instance(valid_webhook_url: str, valid_channel_id: str) -> Any:  # noqa ANN401: typing.Any is not allowed
	"""Fixture providing a WebhookSlack instance with valid parameters.

	Parameters
	----------
	valid_webhook_url : str
		Valid webhook URL from fixture
	valid_channel_id : str
		Valid channel ID from fixture

	Returns
	-------
	Any
		Initialized WebhookSlack instance
	"""
	return WebhookSlack(url_webhook=valid_webhook_url, id_channel=valid_channel_id)


@pytest.fixture
def mock_successful_request() -> MagicMock:
	"""Fixture mocking a successful HTTP request.

	Returns
	-------
	MagicMock
		Mock response with status_code=200 and text="ok"
	"""
	mock_resp = MagicMock()
	mock_resp.status_code = 200
	mock_resp.text = "ok"
	mock_resp.raise_for_status.return_value = None
	return mock_resp


@pytest.fixture
def mock_failed_request() -> MagicMock:
	"""Fixture mocking a failed HTTP request.

	Returns
	-------
	MagicMock
		Mock response that raises HTTPError
	"""
	mock_resp = MagicMock()
	mock_resp.raise_for_status.side_effect = HTTPError("Server error")
	return mock_resp


# --------------------------
# Validation Tests
# --------------------------
class TestValidation:
	"""Test cases for validation methods."""

	def test_validate_url_empty(
		self,
		webhook_slack_instance: WebhookSlack,  # noqa ANN401: typing.Any is not allowed
	) -> None:
		"""Test raises ValueError when URL is empty.

		Verifies
		--------
		That empty URL raises ValueError with appropriate message.

		Parameters
		----------
		webhook_slack_instance : WebhookSlack
			Instance of the WebhookSlack class

		Returns
		-------
		None
		"""
		with pytest.raises(ValueError, match="Webhook URL cannot be empty"):
			webhook_slack_instance._validate_url("")

	def test_validate_url_invalid_format(
		self,
		webhook_slack_instance: WebhookSlack,  # noqa ANN401: typing.Any is not allowed
	) -> None:
		"""Test raises ValueError when URL has invalid format.

		Verifies
		--------
		That non-Slack webhook URL raises ValueError with appropriate message.

		Parameters
		----------
		webhook_slack_instance : WebhookSlack
			Instance of the WebhookSlack class

		Returns
		-------
		None
		"""
		with pytest.raises(ValueError, match="Invalid Slack webhook URL format"):
			webhook_slack_instance._validate_url("https://example.com")

	def test_validate_channel_id_empty(
		self,
		webhook_slack_instance: WebhookSlack,  # noqa ANN401: typing.Any is not allowed
	) -> None:
		"""Test raises ValueError when channel ID is empty.

		Verifies
		--------
		That empty channel ID raises ValueError with appropriate message.

		Parameters
		----------
		webhook_slack_instance : WebhookSlack
			Instance of the WebhookSlack class

		Returns
		-------
		None
		"""
		with pytest.raises(ValueError, match="Channel ID cannot be empty"):
			webhook_slack_instance._validate_channel_id("")

	def test_validate_channel_id_invalid_format(
		self,
		webhook_slack_instance: WebhookSlack,  # noqa ANN401: typing.Any is not allowed
	) -> None:
		"""Test raises ValueError when channel ID has invalid format.

		Verifies
		--------
		That channel ID not starting with # or @ raises ValueError.

		Parameters
		----------
		webhook_slack_instance : WebhookSlack
			Instance of the WebhookSlack class

		Returns
		-------
		None
		"""
		with pytest.raises(ValueError, match="Channel ID must start with # or @"):
			webhook_slack_instance._validate_channel_id("invalid")


# --------------------------
# Initialization Tests
# --------------------------
class TestInitialization:
	"""Test cases for WebhookSlack initialization."""

	def test_init_with_valid_params(self, valid_webhook_url: str, valid_channel_id: str) -> None:
		"""Test successful initialization with valid parameters.

		Verifies
		--------
		That instance is created with correct attribute values.

		Parameters
		----------
		valid_webhook_url : str
			Valid webhook URL from fixture
		valid_channel_id : str
			Valid channel ID from fixture

		Returns
		-------
		None
		"""
		instance = WebhookSlack(
			url_webhook=valid_webhook_url,
			id_channel=valid_channel_id,
			str_username="testbot",
			str_icon_emoji=":test:",
		)
		assert instance.url_webhook == valid_webhook_url
		assert instance.id_channel == valid_channel_id
		assert instance.str_username == "testbot"
		assert instance.str_icon_emoji == ":test:"

	def test_init_with_invalid_url(self, valid_channel_id: str) -> None:
		"""Test raises ValueError with invalid webhook URL.

		Verifies
		--------
		That invalid URL format raises during initialization.

		Parameters
		----------
		valid_channel_id : str
			Valid channel ID from fixture

		Returns
		-------
		None
		"""
		with pytest.raises(ValueError):
			WebhookSlack(url_webhook="invalid-url", id_channel=valid_channel_id)

	def test_init_with_invalid_channel(self, valid_webhook_url: str) -> None:
		"""Test raises ValueError with invalid channel ID.

		Verifies
		--------
		That invalid channel format raises during initialization.

		Parameters
		----------
		valid_webhook_url : str
			Valid webhook URL from fixture

		Returns
		-------
		None
		"""
		with pytest.raises(ValueError):
			WebhookSlack(url_webhook=valid_webhook_url, id_channel="invalid-channel")


# --------------------------
# Message Sending Tests
# --------------------------
class TestSendMessage:
	"""Test cases for send_message method."""

	@patch("requests.request")
	def test_send_message_empty(
		self,
		mock_request: MagicMock,
		webhook_slack_instance: WebhookSlack,  # noqa ANN401: typing.Any is not allowed
	) -> None:
		"""Test raises ValueError when message is empty.

		Verifies
		--------
		That empty message raises ValueError with appropriate message.

		Parameters
		----------
		mock_request : MagicMock
			Mocked requests.request function
		webhook_slack_instance : WebhookSlack
			Instance of the WebhookSlack class

		Returns
		-------
		None
		"""
		with pytest.raises(ValueError, match="Message cannot be empty"):
			webhook_slack_instance.send_message("")
		mock_request.assert_not_called()


# --------------------------
# Type Validation Tests
# --------------------------
class TestTypeValidation:
	"""Test cases for type validation."""

	def test_invalid_url_type(self, valid_channel_id: str) -> None:
		"""Test raises TypeError when URL is not string.

		Verifies
		--------
		That non-string URLs raise TypeError during initialization.

		Parameters
		----------
		valid_channel_id : str
			Valid channel ID from fixture

		Returns
		-------
		None
		"""
		with pytest.raises(TypeError):
			WebhookSlack(
				url_webhook=123,  # type: ignore
				id_channel=valid_channel_id,
			)

	def test_invalid_channel_type(self, valid_webhook_url: str) -> None:
		"""Test raises TypeError when channel ID is not string.

		Verifies
		--------
		That non-string channel IDs raise TypeError during initialization.

		Parameters
		----------
		valid_webhook_url : str
			Valid webhook URL from fixture

		Returns
		-------
		None
		"""
		with pytest.raises(TypeError):
			WebhookSlack(
				url_webhook=valid_webhook_url,
				id_channel=456,  # type: ignore
			)

	def test_invalid_message_type(self, webhook_slack_instance: WebhookSlack) -> None:
		"""Test raises TypeError when message is not string.

		Verifies
		--------
		That non-string messages raise TypeError when sending.

		Parameters
		----------
		webhook_slack_instance : WebhookSlack
			Instance of the WebhookSlack class

		Returns
		-------
		None
		"""
		with pytest.raises(TypeError):
			webhook_slack_instance.send_message(123)  # type: ignore

	def test_invalid_method_type(self, webhook_slack_instance: WebhookSlack) -> None:
		"""Test raises TypeError when method is not string.

		Verifies
		--------
		That non-string methods raise TypeError when sending.

		Parameters
		----------
		webhook_slack_instance : WebhookSlack
			Instance of the WebhookSlack class

		Returns
		-------
		None
		"""
		with pytest.raises(TypeError):
			webhook_slack_instance.send_message("test", 123)  # type: ignore
