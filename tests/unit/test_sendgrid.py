"""Unit tests for SendGrid email integration module.

Tests the SendGrid class functionality including:
- Email parameter validation
- Email sending with various input scenarios
- Error handling and edge cases
"""

import sys
from typing import Optional, TypeVar

import pytest
from pytest_mock import MockerFixture

from stpstone.utils.connections.clouds.sendgrid import SendGrid


# --------------------------
# Constants
# --------------------------
TypeValidEmail = TypeVar(
	"TypeValidEmail", bound=tuple[str, list[str], Optional[list[str]], str, str, str]
)


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def sendgrid_instance() -> SendGrid:
	"""Fixture providing a SendGrid instance.

	Returns
	-------
	SendGrid
		Instance of SendGrid class
	"""
	return SendGrid()


@pytest.fixture
def valid_email_params() -> TypeValidEmail:
	"""Fixture providing valid email parameters.

	Returns
	-------
	TypeValidEmail
		Tuple containing:
		- sender: Valid sender email
		- recipients: list of recipient emails
		- cc: Optional list of CC emails
		- subject: Email subject
		- html_body: HTML content
		- token: API token
	"""
	return (
		"sender@example.com",
		["recipient@example.com"],
		None,
		"Test Subject",
		"<p>Test Content</p>",
		"valid_token",
	)


@pytest.fixture
def mock_sendgrid_client(mocker: MockerFixture) -> object:
	"""Fixture mocking SendGridAPIClient.

	Parameters
	----------
	mocker : MockerFixture
		Pytest-mock fixture for creating mocks

	Returns
	-------
	object
		Mocked SendGridAPIClient instance
	"""
	mock_client = mocker.patch("stpstone.utils.connections.clouds.sendgrid.SendGridAPIClient")
	mock_response = mocker.Mock()
	mock_response.status_code = 202
	mock_client.return_value.send.return_value = mock_response
	return mock_client


# --------------------------
# Tests for _validate_email_params
# --------------------------
def test_validate_email_params_valid_input(
	sendgrid_instance: SendGrid, valid_email_params: TypeValidEmail
) -> None:
	"""Test _validate_email_params with valid inputs.

	Verifies
	--------
	That valid parameters pass validation without raising exceptions.

	Parameters
	----------
	sendgrid_instance : SendGrid
		SendGrid class instance
	valid_email_params : TypeValidEmail
		Valid email parameters from fixture

	Returns
	-------
	None
	"""
	str_sender, list_recipients, _, subject, html_body, token = valid_email_params
	sendgrid_instance._validate_email_params(
		str_sender, list_recipients, subject, html_body, token
	)


def test_validate_email_params_empty_sender(sendgrid_instance: SendGrid) -> None:
	"""Test _validate_email_params with empty sender email.

	Verifies
	--------
	That empty sender email raises ValueError with appropriate message.

	Parameters
	----------
	sendgrid_instance : SendGrid
		SendGrid class instance

	Returns
	-------
	None
	"""
	with pytest.raises(ValueError, match="Sender email cannot be empty"):
		sendgrid_instance._validate_email_params(
			"", ["recipient@example.com"], "Subject", "<p>Content</p>", "token"
		)


def test_validate_email_params_empty_recipients(sendgrid_instance: SendGrid) -> None:
	"""Test _validate_email_params with empty recipients list.

	Verifies
	--------
	That empty recipients list raises ValueError with appropriate message.

	Parameters
	----------
	sendgrid_instance : SendGrid
		SendGrid class instance

	Returns
	-------
	None
	"""
	with pytest.raises(ValueError, match="Recipients list cannot be empty"):
		sendgrid_instance._validate_email_params(
			"sender@example.com", [], "Subject", "<p>Content</p>", "token"
		)


def test_validate_email_params_empty_subject(sendgrid_instance: SendGrid) -> None:
	"""Test _validate_email_params with empty subject.

	Verifies
	--------
	That empty subject raises ValueError with appropriate message.

	Parameters
	----------
	sendgrid_instance : SendGrid
		SendGrid class instance

	Returns
	-------
	None
	"""
	with pytest.raises(ValueError, match="Email subject cannot be empty"):
		sendgrid_instance._validate_email_params(
			"sender@example.com", ["recipient@example.com"], "", "<p>Content</p>", "token"
		)


def test_validate_email_params_empty_html_body(sendgrid_instance: SendGrid) -> None:
	"""Test _validate_email_params with empty HTML body.

	Verifies
	--------
	That empty HTML body raises ValueError with appropriate message.

	Parameters
	----------
	sendgrid_instance : SendGrid
		SendGrid class instance

	Returns
	-------
	None
	"""
	with pytest.raises(ValueError, match="HTML body cannot be empty"):
		sendgrid_instance._validate_email_params(
			"sender@example.com", ["recipient@example.com"], "Subject", "", "token"
		)


def test_validate_email_params_empty_token(sendgrid_instance: SendGrid) -> None:
	"""Test _validate_email_params with empty API token.

	Verifies
	--------
	That empty API token raises ValueError with appropriate message.

	Parameters
	----------
	sendgrid_instance : SendGrid
		SendGrid class instance

	Returns
	-------
	None
	"""
	with pytest.raises(ValueError, match="API token cannot be empty"):
		sendgrid_instance._validate_email_params(
			"sender@example.com", ["recipient@example.com"], "Subject", "<p>Content</p>", ""
		)


# --------------------------
# Tests for send_email
# --------------------------
def test_send_email_success(
	sendgrid_instance: SendGrid, valid_email_params: TypeValidEmail, mock_sendgrid_client: object
) -> None:
	"""Test send_email with valid inputs and successful API call.

	Verifies
	--------
	That email sending succeeds and returns True when API call returns 202 status.

	Parameters
	----------
	sendgrid_instance : SendGrid
		SendGrid class instance
	valid_email_params : TypeValidEmail
		Valid email parameters from fixture
	mock_sendgrid_client : object
		Mocked SendGridAPIClient instance

	Returns
	-------
	None
	"""
	str_sender, list_recipients, list_cc, subject, html_body, token = valid_email_params
	result = sendgrid_instance.send_email(
		str_sender, list_recipients, list_cc, subject, html_body, token
	)
	assert result is True
	mock_sendgrid_client.return_value.send.assert_called_once()


def test_send_email_with_cc(
	sendgrid_instance: SendGrid, valid_email_params: TypeValidEmail, mock_sendgrid_client: object
) -> None:
	"""Test send_email with CC recipients.

	Verifies
	--------
	That email sending with CC recipients works correctly and returns True.

	Parameters
	----------
	sendgrid_instance : SendGrid
		SendGrid class instance
	valid_email_params : TypeValidEmail
		Valid email parameters from fixture
	mock_sendgrid_client : object
		Mocked SendGridAPIClient instance

	Returns
	-------
	None
	"""
	str_sender, list_recipients, _, subject, html_body, token = valid_email_params
	list_cc = ["cc@example.com"]
	result = sendgrid_instance.send_email(
		str_sender, list_recipients, list_cc, subject, html_body, token
	)
	assert result is True
	mock_sendgrid_client.return_value.send.assert_called_once()


def test_send_email_api_failure(
	sendgrid_instance: SendGrid, valid_email_params: TypeValidEmail, mocker: MockerFixture
) -> None:
	"""Test send_email when API call fails.

	Verifies
	--------
	That API failure raises Exception with appropriate message.

	Parameters
	----------
	sendgrid_instance : SendGrid
		SendGrid class instance
	valid_email_params : TypeValidEmail
		Valid email parameters from fixture
	mocker : MockerFixture
		Pytest-mock fixture for creating mocks

	Returns
	-------
	None
	"""
	mocker.patch(
		"stpstone.utils.connections.clouds.sendgrid.SendGridAPIClient",
		side_effect=ValueError("Error sending email from sendgrid: API failure"),
	)
	str_sender, list_recipients, list_cc, subject, html_body, token = valid_email_params
	with pytest.raises(ValueError, match="Error sending email from sendgrid: API failure"):
		sendgrid_instance.send_email(
			str_sender, list_recipients, list_cc, subject, html_body, token
		)


def test_send_email_invalid_status_code(
	sendgrid_instance: SendGrid, valid_email_params: TypeValidEmail, mocker: MockerFixture
) -> None:
	"""Test send_email when API returns non-202 status code.

	Verifies
	--------
	That non-202 status code returns False.

	Parameters
	----------
	sendgrid_instance : SendGrid
		SendGrid class instance
	valid_email_params : TypeValidEmail
		Valid email parameters from fixture
	mocker : MockerFixture
		Pytest-mock fixture for creating mocks

	Returns
	-------
	None
	"""
	mock_client = mocker.patch("stpstone.utils.connections.clouds.sendgrid.SendGridAPIClient")
	mock_response = mocker.Mock()
	mock_response.status_code = 400
	mock_client.return_value.send.return_value = mock_response
	str_sender, list_recipients, list_cc, subject, html_body, token = valid_email_params
	result = sendgrid_instance.send_email(
		str_sender, list_recipients, list_cc, subject, html_body, token
	)
	assert result is False
	mock_client.return_value.send.assert_called_once()


def test_send_email_empty_cc(
	sendgrid_instance: SendGrid, valid_email_params: TypeValidEmail, mock_sendgrid_client: object
) -> None:
	"""Test send_email with empty CC list.

	Verifies
	--------
	That empty CC list is handled correctly and email sends successfully.

	Parameters
	----------
	sendgrid_instance : SendGrid
		SendGrid class instance
	valid_email_params : TypeValidEmail
		Valid email parameters from fixture
	mock_sendgrid_client : object
		Mocked SendGridAPIClient instance

	Returns
	-------
	None
	"""
	str_sender, list_recipients, _, subject, html_body, token = valid_email_params
	result = sendgrid_instance.send_email(
		str_sender, list_recipients, [], subject, html_body, token
	)
	assert result is True
	mock_sendgrid_client.return_value.send.assert_called_once()


def test_send_email_multiple_recipients(
	sendgrid_instance: SendGrid, valid_email_params: TypeValidEmail, mock_sendgrid_client: object
) -> None:
	"""Test send_email with multiple recipients.

	Verifies
	--------
	That multiple recipients are handled correctly and email sends successfully.

	Parameters
	----------
	sendgrid_instance : SendGrid
		SendGrid class instance
	valid_email_params : TypeValidEmail
		Valid email parameters from fixture
	mock_sendgrid_client : object
		Mocked SendGridAPIClient instance

	Returns
	-------
	None
	"""
	str_sender, _, list_cc, subject, html_body, token = valid_email_params
	list_recipients = ["recipient1@example.com", "recipient2@example.com"]
	result = sendgrid_instance.send_email(
		str_sender, list_recipients, list_cc, subject, html_body, token
	)
	assert result is True
	mock_sendgrid_client.return_value.send.assert_called_once()


# --------------------------
# Edge Cases and Type Validation
# --------------------------
def test_send_email_none_inputs(
	sendgrid_instance: SendGrid, valid_email_params: TypeValidEmail
) -> None:
	"""Test send_email with None inputs.

	Verifies
	--------
	That None inputs raise appropriate ValueError exceptions.

	Parameters
	----------
	sendgrid_instance : SendGrid
		SendGrid class instance
	valid_email_params : TypeValidEmail
		Valid email parameters from fixture

	Returns
	-------
	None
	"""
	str_sender, list_recipients, list_cc, subject, html_body, token = valid_email_params
	with pytest.raises(ValueError, match="Sender email cannot be empty"):
		sendgrid_instance.send_email(None, list_recipients, list_cc, subject, html_body, token)
	with pytest.raises(TypeError, match="must be of type"):
		sendgrid_instance.send_email(str_sender, None, list_cc, subject, html_body, token)
	with pytest.raises(ValueError, match="Email subject cannot be empty"):
		sendgrid_instance.send_email(str_sender, list_recipients, list_cc, None, html_body, token)
	with pytest.raises(ValueError, match="HTML body cannot be empty"):
		sendgrid_instance.send_email(str_sender, list_recipients, list_cc, subject, None, token)
	with pytest.raises(ValueError, match="API token cannot be empty"):
		sendgrid_instance.send_email(
			str_sender, list_recipients, list_cc, subject, html_body, None
		)


def test_send_email_invalid_types(
	sendgrid_instance: SendGrid, valid_email_params: TypeValidEmail
) -> None:
	"""Test send_email with invalid input types.

	Verifies
	--------
	That invalid input types raise appropriate ValueError exceptions.

	Parameters
	----------
	sendgrid_instance : SendGrid
		SendGrid class instance
	valid_email_params : TypeValidEmail
		Valid email parameters from fixture

	Returns
	-------
	None
	"""
	str_sender, list_recipients, list_cc, subject, html_body, token = valid_email_params
	with pytest.raises(
		ValueError,
		match="Error sending email from sendgrid: 'int' " + "object has no attribute 'get'",
	):
		sendgrid_instance.send_email(123, list_recipients, list_cc, subject, html_body, token)
	with pytest.raises(TypeError, match="must be of type"):
		sendgrid_instance.send_email(str_sender, "not_a_list", list_cc, subject, html_body, token)
	with pytest.raises(
		ValueError, match="Error sending email from sendgrid: HTTP Error 401: " + "Unauthorized"
	):
		sendgrid_instance.send_email(str_sender, list_recipients, list_cc, 123, html_body, token)
	with pytest.raises(AttributeError, match="'int' object has no attribute 'mime_type'"):
		sendgrid_instance.send_email(str_sender, list_recipients, list_cc, subject, 123, token)
	with pytest.raises(
		ValueError, match="Error sending email from sendgrid: HTTP Error 401: " + "Unauthorized"
	):
		sendgrid_instance.send_email(str_sender, list_recipients, list_cc, subject, html_body, 123)


# --------------------------
# Reload Logic
# --------------------------
def test_module_reload(mocker: MockerFixture) -> None:
	"""Test module reload behavior.

	Verifies
	--------
	That module can be reloaded without errors and retains functionality.

	Parameters
	----------
	mocker : MockerFixture
		Pytest-mock fixture for creating mocks

	Returns
	-------
	None
	"""
	import importlib

	mock_client = mocker.patch("sendgrid.SendGridAPIClient")
	mock_response = mocker.Mock()
	mock_response.status_code = 202
	mock_client.return_value.send.return_value = mock_response

	importlib.reload(sys.modules[SendGrid.__module__])
	sendgrid_instance = SendGrid()
	result = sendgrid_instance.send_email(
		"sender@example.com", ["recipient@example.com"], None, "Subject", "<p>Content</p>", "token"
	)
	assert result is True
	mock_client.return_value.send.assert_called_once()
