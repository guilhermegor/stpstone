"""Unit tests for DealingOutlook class.

Tests the Outlook email interaction functionality including:
- Email sending with various configurations
- Attachment downloading
- Email content retrieval
- Automated replies
- Validation methods
"""

import platform

import pytest


if platform.system() != "Windows":
	pytest.skip("Outlook tests require Windows", allow_module_level=True)

import os
from typing import Any, Union
from unittest.mock import MagicMock, patch

from pytest_mock import MockerFixture

from stpstone.utils.microsoft_apps.outlook import DealingOutlook
from stpstone.utils.parsers.folders import DirFilesManagement
from stpstone.utils.parsers.json import JsonFiles
from stpstone.utils.parsers.str import StrHandler


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def outlook_mock(mocker: MockerFixture) -> MagicMock:
	"""Fixture providing mocked Outlook application.

	Parameters
	----------
	mocker : MockerFixture
		Pytest-mock fixture for creating mocks

	Returns
	-------
	MagicMock
		Mocked Outlook application object
	"""
	mock = mocker.MagicMock()
	mocker.patch("stpstone.utils.microsoft_apps.outlook.win32.Dispatch", return_value=mock)
	return mock


@pytest.fixture
def dealing_outlook(outlook_mock: MagicMock) -> DealingOutlook:
	"""Fixture providing DealingOutlook instance with mocked Outlook.

	Parameters
	----------
	outlook_mock : MagicMock
		Mocked Outlook application

	Returns
	-------
	DealingOutlook
		DealingOutlook instance with mocked dependencies
	"""
	return DealingOutlook()


@pytest.fixture
def mail_item_mock() -> MagicMock:
	"""Fixture providing mocked MailItem object.

	Returns
	-------
	MagicMock
		Mocked MailItem object
	"""
	return MagicMock()


@pytest.fixture
def namespace_mock() -> MagicMock:
	"""Fixture providing mocked MAPI namespace.

	Returns
	-------
	MagicMock
		Mocked MAPI namespace object
	"""
	return MagicMock()


@pytest.fixture
def folder_mock() -> MagicMock:
	"""Fixture providing mocked Outlook folder.

	Returns
	-------
	MagicMock
		Mocked Outlook folder object
	"""
	return MagicMock()


# --------------------------
# Validation Tests
# --------------------------
class TestValidationMethods:
	"""Tests for validation helper methods."""

	def test_validate_email_account_valid(
		self,
		dealing_outlook: Any,  # noqa ANN401: typing.Any not allowed
	) -> None:
		"""Test valid email account passes validation.

		Verifies
		--------
		- Valid email account with '@' passes validation
		- No exceptions are raised

		Parameters
		----------
		dealing_outlook : Any
			DealingOutlook instance

		Returns
		-------
		None
		"""
		dealing_outlook._validate_email_account("valid@example.com")

	@pytest.mark.parametrize("email", ["", "invalid", None])
	def test_validate_email_account_invalid(
		self,
		dealing_outlook: Any,  # noqa ANN401: typing.Any not allowed
		email: str,
	) -> None:
		"""Test invalid email accounts raise ValueError.

		Verifies
		--------
		- Empty, invalid format or None emails raise ValueError
		- Error messages are appropriate

		Parameters
		----------
		dealing_outlook : Any
			DealingOutlook instance
		email : str
			Invalid email

		Returns
		-------
		None
		"""
		with pytest.raises((TypeError, ValueError)):
			dealing_outlook._validate_email_account(email)

	def test_validate_folder_path_valid(
		self,
		dealing_outlook: Any,  # noqa ANN401: typing.Any not allowed
	) -> None:
		"""Test valid folder path passes validation.

		Verifies
		--------
		- Non-empty folder path passes validation
		- No exceptions are raised

		Parameters
		----------
		dealing_outlook : Any
			DealingOutlook instance

		Returns
		-------
		None
		"""
		dealing_outlook._validate_folder_path("Inbox")

	@pytest.mark.parametrize("path", ["", None])
	def test_validate_folder_path_invalid(
		self,
		dealing_outlook: Any,  # noqa ANN401: typing.Any not allowed
		path: str,
	) -> None:
		"""Test invalid folder paths raise ValueError.

		Verifies
		--------
		- Empty or None folder paths raise ValueError
		- Error messages are appropriate

		Parameters
		----------
		dealing_outlook : Any
			DealingOutlook instance
		path : str
			Invalid folder path

		Returns
		-------
		None
		"""
		with pytest.raises((TypeError, ValueError)):
			dealing_outlook._validate_folder_path(path)

	@pytest.mark.parametrize("path", ["/valid/path", ["/valid/path1", "/valid/path2"]])
	def test_validate_attachment_path_valid(
		self,
		dealing_outlook: Any,  # noqa ANN401: typing.Any not allowed
		path: Union[str, list[str]],
	) -> None:
		"""Test valid attachment paths pass validation.

		Verifies
		--------
		- Both string and list paths pass validation
		- No exceptions are raised

		Parameters
		----------
		dealing_outlook : Any
			DealingOutlook instance
		path : Union[str, list[str]]
			Valid attachment path

		Returns
		-------
		None
		"""
		dealing_outlook._validate_attachment_path(path)

	@pytest.mark.parametrize("path", [123, [123, 456], ["valid", 123], None])
	def test_validate_attachment_path_invalid(
		self,
		dealing_outlook: Any,  # noqa ANN401: typing.Any not allowed
		path: Any,  # noqa ANN401: typing.Any not allowed
	) -> None:
		"""Test invalid attachment paths raise ValueError.

		Verifies
		--------
		- Non-string paths raise ValueError
		- Mixed type lists raise ValueError
		- None raises ValueError

		Parameters
		----------
		dealing_outlook : Any
			DealingOutlook instance
		path : Any
			Invalid attachment path

		Returns
		-------
		None
		"""
		with pytest.raises((TypeError, ValueError)):
			dealing_outlook._validate_attachment_path(path)


# --------------------------
# Send Email Tests
# --------------------------
class TestSendEmail:
	"""Tests for send_email method."""

	def test_send_email_basic(
		self,
		dealing_outlook: Any,  # noqa ANN401: typing.Any not allowed
		outlook_mock: MagicMock,
		mail_item_mock: MagicMock,
	) -> None:
		"""Test basic email sending functionality.

		Verifies
		--------
		- Email is created with correct subject
		- Display is called when auto_send is False

		Parameters
		----------
		dealing_outlook : Any
			DealingOutlook instance
		outlook_mock : MagicMock
			Mocked outlook object
		mail_item_mock : MagicMock
			Mocked mail item object

		Returns
		-------
		None
		"""
		outlook_mock.CreateItem.return_value = mail_item_mock
		dealing_outlook.send_email("Test Subject")
		mail_item_mock.Display.assert_called_once()

	def test_send_email_with_attachments(
		self,
		dealing_outlook: Any,  # noqa ANN401: typing.Any not allowed
		outlook_mock: MagicMock,
		mail_item_mock: MagicMock,
		tmp_path: Any,  # noqa ANN401: typing.Any not allowed
	) -> None:
		"""Test email with attachments.

		Verifies
		--------
		- Attachments are added when files exist
		- Non-existent attachments are skipped

		Parameters
		----------
		dealing_outlook : Any
			DealingOutlook instance
		outlook_mock : MagicMock
			Mocked outlook object
		mail_item_mock : MagicMock
			Mocked mail item object
		tmp_path : Any
			Temporary directory path

		Returns
		-------
		None
		"""
		outlook_mock.CreateItem.return_value = mail_item_mock
		test_file = tmp_path / "test.txt"
		test_file.write_text("test content")

		dealing_outlook.send_email(
			"Test Subject", mail_attachments=[str(test_file), "nonexistent.txt"]
		)
		mail_item_mock.Attachments.Add.assert_called_once_with(Source=str(test_file))

	def test_send_email_auto_send(
		self,
		dealing_outlook: Any,  # noqa ANN401: typing.Any not allowed
		outlook_mock: MagicMock,
		mail_item_mock: MagicMock,
	) -> None:
		"""Test auto-send functionality.

		Verifies
		--------
		- Send is called when auto_send is True
		- Display is not called

		Parameters
		----------
		dealing_outlook : Any
			DealingOutlook instance
		outlook_mock : MagicMock
			Mocked outlook object
		mail_item_mock : MagicMock
			Mocked mail item object

		Returns
		-------
		None
		"""
		outlook_mock.CreateItem.return_value = mail_item_mock
		dealing_outlook.send_email("Test Subject", auto_send_email=True)
		mail_item_mock.Send.assert_called_once()
		mail_item_mock.Display.assert_not_called()

	def test_send_email_on_behalf(
		self,
		dealing_outlook: Any,  # noqa ANN401: typing.Any not allowed
		outlook_mock: MagicMock,
		mail_item_mock: MagicMock,
	) -> None:
		"""Test send-on-behalf functionality.

		Verifies
		--------
		- SentOnBehalfOfName is set correctly
		- Account lookup is performed

		Parameters
		----------
		dealing_outlook : Any
			DealingOutlook instance
		outlook_mock : MagicMock
			Mocked outlook object
		mail_item_mock : MagicMock
			Mocked mail item object

		Returns
		-------
		None
		"""
		account_mock = MagicMock()
		account_mock.__str__.return_value = "other@example.com"
		outlook_mock.Session.Accounts = [account_mock]
		outlook_mock.CreateItem.return_value = mail_item_mock

		dealing_outlook.send_email("Test Subject", send_behalf_of="other@example.com")
		assert mail_item_mock.SentOnBehalfOfName == "other@example.com"

	def test_send_email_error_handling(
		self,
		dealing_outlook: Any,  # noqa ANN401: typing.Any not allowed
		outlook_mock: MagicMock,
		mail_item_mock: MagicMock,
	) -> None:
		"""Test error handling during send.

		Verifies
		--------
		- Exceptions during send are properly wrapped
		- Error message is preserved

		Parameters
		----------
		dealing_outlook : Any
			DealingOutlook instance
		outlook_mock : MagicMock
			Mocked outlook object
		mail_item_mock : MagicMock
			Mocked mail item object

		Returns
		-------
		None
		"""
		outlook_mock.CreateItem.return_value = mail_item_mock
		mail_item_mock.Send.side_effect = Exception("Send failed")

		with pytest.raises(RuntimeError, match="Send failed"):
			dealing_outlook.send_email("Test Subject", auto_send_email=True)


# --------------------------
# Download Attachment Tests
# --------------------------
class TestDownloadAttachment:
	"""Tests for download_attch method."""

	def test_download_attachment_basic(
		self,
		dealing_outlook: Any,  # noqa ANN401: typing.Any not allowed
		outlook_mock: MagicMock,
		namespace_mock: MagicMock,
		folder_mock: MagicMock,
		tmp_path: Any,  # noqa ANN401: typing.Any not allowed
	) -> None:
		"""Test basic attachment download.

		Verifies
		--------
		- Attachment is saved to specified path
		- Status dictionary reflects success

		Parameters
		----------
		dealing_outlook : Any
			DealingOutlook instance
		outlook_mock : MagicMock
			Mocked outlook object
		namespace_mock : MagicMock
			Mocked namespace object
		folder_mock : MagicMock
			Mocked folder object
		tmp_path : Any
			Temporary directory path

		Returns
		-------
		None
		"""
		# Setup mocks
		outlook_mock.GetNamespace.return_value = namespace_mock
		account_folder = MagicMock()
		namespace_mock.Folders.__getitem__.return_value = account_folder
		account_folder.Folders.__getitem__.return_value = folder_mock

		message_mock = MagicMock()
		attachment_mock = MagicMock()
		attachment_mock.FileName = "test.txt"
		message_mock.Attachments = [attachment_mock]
		message_mock.Subject = "Test Subject"
		folder_mock.Items.Count = 1
		folder_mock.Items.__getitem__.return_value = message_mock

		with (
			patch.object(DirFilesManagement, "object_exists", return_value=True),
			patch.object(JsonFiles, "send_json", side_effect=lambda x: x),
		):
			save_path = str(tmp_path / "attachment.txt")
			result = dealing_outlook.download_attch("test@example.com", "Inbox", "Test", save_path)

			assert result[save_path] is True
			attachment_mock.SaveAsFile.assert_called_once_with(save_path)

	def test_download_attachment_multiple_paths(
		self,
		dealing_outlook: Any,  # noqa ANN401: typing.Any not allowed
		outlook_mock: MagicMock,
		namespace_mock: MagicMock,
		folder_mock: MagicMock,
		tmp_path: Any,  # noqa ANN401: typing.Any not allowed
	) -> None:
		"""Test attachment download with multiple save paths.

		Verifies
		--------
		- Attachment is saved to all specified paths
		- Status dictionary reflects all attempts

		Parameters
		----------
		dealing_outlook : Any
			DealingOutlook instance
		outlook_mock : MagicMock
			Mocked outlook object
		namespace_mock : MagicMock
			Mocked namespace object
		folder_mock : MagicMock
			Mocked folder object
		tmp_path : Any
			Temporary directory path

		Returns
		-------
		None
		"""
		# Setup mocks
		outlook_mock.GetNamespace.return_value = namespace_mock
		account_folder = MagicMock()
		namespace_mock.Folders.__getitem__.return_value = account_folder
		account_folder.Folders.__getitem__.return_value = folder_mock

		message_mock = MagicMock()
		attachment_mock = MagicMock()
		attachment_mock.FileName = "test.txt"
		message_mock.Attachments = [attachment_mock]
		message_mock.Subject = "Test Subject"
		folder_mock.Items.Count = 1
		folder_mock.Items.__getitem__.return_value = message_mock

		with (
			patch.object(DirFilesManagement, "object_exists", return_value=True),
			patch.object(JsonFiles, "send_json", side_effect=lambda x: x),
		):
			paths = [str(tmp_path / "attachment1.txt"), str(tmp_path / "attachment2.txt")]
			result = dealing_outlook.download_attch("test@example.com", "Inbox", "Test", paths)

			assert len(result) == 2
			assert all(status is True for status in result.values())

	def test_download_attachment_with_original_name(
		self,
		dealing_outlook: Any,  # noqa ANN401: typing.Any not allowed
		outlook_mock: MagicMock,
		namespace_mock: MagicMock,
		folder_mock: MagicMock,
		tmp_path: Any,  # noqa ANN401: typing.Any not allowed
	) -> None:
		"""Test download with original filename.

		Verifies
		--------
		- Attachment is saved with original filename
		- File format filtering works

		Parameters
		----------
		dealing_outlook : Any
			DealingOutlook instance
		outlook_mock : MagicMock
			Mocked outlook object
		namespace_mock : MagicMock
			Mocked namespace object
		folder_mock : MagicMock
			Mocked folder object
		tmp_path : Any
			Temporary directory path

		Returns
		-------
		None
		"""
		# Setup mocks
		outlook_mock.GetNamespace.return_value = namespace_mock
		account_folder = MagicMock()
		namespace_mock.Folders.__getitem__.return_value = account_folder
		account_folder.Folders.__getitem__.return_value = folder_mock

		message_mock = MagicMock()
		attachment_mock = MagicMock()
		attachment_mock.FileName = "test.txt"
		message_mock.Attachments = [attachment_mock]
		message_mock.Subject = "Test Subject"
		folder_mock.Items.Count = 1
		folder_mock.Items.__getitem__.return_value = message_mock

		with (
			patch.object(DirFilesManagement, "get_file_format_from_file_name", return_value="txt"),
			patch.object(DirFilesManagement, "object_exists", return_value=True),
			patch.object(JsonFiles, "send_json", side_effect=lambda x: x),
		):
			save_dir = str(tmp_path)
			result = dealing_outlook.download_attch(
				"test@example.com",
				"Inbox",
				"Test",
				save_dir,
				bool_save_file_w_original_name=True,
				list_fileformat=["txt"],
			)

			expected_path = os.path.join(save_dir, "test.txt")
			assert expected_path in result
			attachment_mock.SaveAsFile.assert_called_once_with(expected_path)


# --------------------------
# Email Retrieval Tests
# --------------------------
class TestEmailRetrieval:
	"""Tests for email retrieval methods."""

	def test_received_email_subject_w_rule_subject(
		self,
		dealing_outlook: Any,  # noqa ANN401: typing.Any not allowed
		outlook_mock: MagicMock,
		namespace_mock: MagicMock,
		folder_mock: MagicMock,
	) -> None:
		"""Test subject retrieval with subject output format.

		Verifies
		--------
		- Correct subjects are returned
		- Only matching emails are included

		Parameters
		----------
		dealing_outlook : Any
			DealingOutlook instance
		outlook_mock : MagicMock
			Mocked outlook object
		namespace_mock : MagicMock
			Mocked namespace object
		folder_mock : MagicMock
			Mocked folder object

		Returns
		-------
		None
		"""
		# Setup mocks
		outlook_mock.GetNamespace.return_value = namespace_mock
		account_folder = MagicMock()
		namespace_mock.Folders.__getitem__.return_value = account_folder
		account_folder.Folders.__getitem__.return_value = folder_mock

		message1 = MagicMock()
		message1.Subject = "Test Message 1"
		message2 = MagicMock()
		message2.Subject = "Other Message"
		messages = [message1, message2]
		folder_mock.Items.Count = 2
		folder_mock.Items.__getitem__.side_effect = lambda i: messages[i]

		# Mock string matching
		with patch.object(StrHandler, "find_substr_str", side_effect=lambda s, p: p in s):
			result = dealing_outlook.received_email_subject_w_rule(
				"test@example.com", "Inbox", "Test", output="subject"
			)

			assert result == ["Test Message 1"]

	def test_received_email_subject_w_rule_properties(
		self,
		dealing_outlook: Any,  # noqa ANN401: typing.Any not allowed
		outlook_mock: MagicMock,
		namespace_mock: MagicMock,
		folder_mock: MagicMock,
	) -> None:
		"""Test subject retrieval with properties output format.

		Verifies
		--------
		- Correct properties are returned
		- Dictionary structure is correct

		Parameters
		----------
		dealing_outlook : Any
			DealingOutlook instance
		outlook_mock : MagicMock
			Mocked outlook object
		namespace_mock : MagicMock
			Mocked namespace object
		folder_mock : MagicMock
			Mocked folder object

		Returns
		-------
		None
		"""
		# Setup mocks
		outlook_mock.GetNamespace.return_value = namespace_mock
		account_folder = MagicMock()
		namespace_mock.Folders.__getitem__.return_value = account_folder
		account_folder.Folders.__getitem__.return_value = folder_mock

		message = MagicMock()
		message.Subject = "Test Message"
		message.LastModificationTime = "2023-01-01"
		message.CreationTime = "2023-01-01"
		folder_mock.Items.Count = 1
		folder_mock.Items.__getitem__.return_value = message

		# Mock string matching
		with patch.object(StrHandler, "find_substr_str", return_value=True):
			result = dealing_outlook.received_email_subject_w_rule(
				"test@example.com", "Inbox", "Test", output="properties"
			)

			assert result[0]["subject"] == "Test Message"
			assert "last_edition" in result[0]
			assert "creation_time" in result[0]

	def test_get_body_content(
		self,
		dealing_outlook: Any,  # noqa ANN401: typing.Any not allowed
		outlook_mock: MagicMock,
		namespace_mock: MagicMock,
		folder_mock: MagicMock,
	) -> None:
		"""Test email body content retrieval.

		Verifies
		--------
		- Correct body content is returned
		- Metadata is included

		Parameters
		----------
		dealing_outlook : Any
			DealingOutlook instance
		outlook_mock : MagicMock
			Mocked outlook object
		namespace_mock : MagicMock
			Mocked namespace object
		folder_mock : MagicMock
			Mocked folder object

		Returns
		-------
		None
		"""
		# Setup mocks
		outlook_mock.GetNamespace.return_value = namespace_mock
		account_folder = MagicMock()
		namespace_mock.Folders.__getitem__.return_value = account_folder
		account_folder.Folders.__getitem__.return_value = folder_mock

		message = MagicMock()
		message.Subject = "Test Message"
		message.LastModificationTime = "2023-01-01"
		message.CreationTime = "2023-01-01"
		message.body = "Test body content"
		folder_mock.Items.Count = 1
		folder_mock.Items.__getitem__.return_value = message

		# Mock string matching
		with patch.object(StrHandler, "find_substr_str", return_value=True):
			result = dealing_outlook.get_body_content("test@example.com", "Inbox", "Test")

			assert result[0]["body"] == "Test body content"
			assert result[0]["subject"] == "Test Message"


# --------------------------
# Reply Email Tests
# --------------------------
class TestReplyEmail:
	"""Tests for reply_email method."""

	def test_reply_email_basic(
		self,
		dealing_outlook: Any,  # noqa ANN401: typing.Any not allowed
		outlook_mock: MagicMock,
		namespace_mock: MagicMock,
		folder_mock: MagicMock,
	) -> None:
		"""Test basic email reply functionality.

		Verifies
		--------
		- Reply is created for matching email
		- Body is properly appended
		- Display is called when auto_send is False

		Parameters
		----------
		dealing_outlook : Any
			DealingOutlook instance
		outlook_mock : MagicMock
			Mocked outlook object
		namespace_mock : MagicMock
			Mocked namespace object
		folder_mock : MagicMock
			Mocked folder object

		Returns
		-------
		None
		"""
		# Setup mocks
		outlook_mock.GetNamespace.return_value = namespace_mock
		account_folder = MagicMock()
		namespace_mock.Folders.__getitem__.return_value = account_folder
		account_folder.Folders.__getitem__.return_value = folder_mock

		message = MagicMock()
		message.Subject = "Test Message"
		reply_mock = MagicMock()
		message.Reply.return_value = reply_mock
		folder_mock.Items.Count = 1
		folder_mock.Items.__getitem__.return_value = message

		# Capture original HTMLBody mock before call — augmented assignment (+=)
		# replaces reply_mock.HTMLBody with the __iadd__ return value, so assertions
		# must be made on the original object, not the re-bound attribute.
		original_html_body = reply_mock.HTMLBody

		dealing_outlook.reply_email("test@example.com", "Inbox", "Test Message", "Reply content")

		original_html_body.__iadd__.assert_called_once_with("Reply content")
		reply_mock.Display.assert_called_once()

	def test_reply_email_with_cc_bcc(
		self,
		dealing_outlook: Any,  # noqa ANN401: typing.Any not allowed
		outlook_mock: MagicMock,
		namespace_mock: MagicMock,
		folder_mock: MagicMock,
	) -> None:
		"""Test email reply with CC and BCC.

		Verifies
		--------
		- CC and BCC are set correctly

		Parameters
		----------
		dealing_outlook : Any
			DealingOutlook instance
		outlook_mock : MagicMock
			Mocked outlook object
		namespace_mock : MagicMock
			Mocked namespace object
		folder_mock : MagicMock
			Mocked folder object

		Returns
		-------
		None
		"""
		# Setup mocks
		outlook_mock.GetNamespace.return_value = namespace_mock
		account_folder = MagicMock()
		namespace_mock.Folders.__getitem__.return_value = account_folder
		account_folder.Folders.__getitem__.return_value = folder_mock

		message = MagicMock()
		message.Subject = "Test Message"
		reply_mock = MagicMock()
		message.Reply.return_value = reply_mock
		folder_mock.Items.Count = 1
		folder_mock.Items.__getitem__.return_value = message

		dealing_outlook.reply_email(
			"test@example.com",
			"Inbox",
			"Test Message",
			"Reply content",
			mail_cc="cc@example.com",
			mail_bcc="bcc@example.com",
		)

		assert reply_mock.CC == "cc@example.com"
		assert reply_mock.BCC == "bcc@example.com"

	def test_reply_email_auto_send(
		self,
		dealing_outlook: Any,  # noqa ANN401: typing.Any not allowed
		outlook_mock: MagicMock,
		namespace_mock: MagicMock,
		folder_mock: MagicMock,
	) -> None:
		"""Test auto-send functionality for replies.

		Verifies
		--------
		- Send is called when auto_send is True
		- Display is not called unless bool_image_display is True

		Parameters
		----------
		dealing_outlook : Any
			DealingOutlook instance
		outlook_mock : MagicMock
			Mocked outlook object
		namespace_mock : MagicMock
			Mocked namespace object
		folder_mock : MagicMock
			Mocked folder object

		Returns
		-------
		None
		"""
		# Setup mocks
		outlook_mock.GetNamespace.return_value = namespace_mock
		account_folder = MagicMock()
		namespace_mock.Folders.__getitem__.return_value = account_folder
		account_folder.Folders.__getitem__.return_value = folder_mock

		message = MagicMock()
		message.Subject = "Test Message"
		reply_mock = MagicMock()
		message.Reply.return_value = reply_mock
		folder_mock.Items.Count = 1
		folder_mock.Items.__getitem__.return_value = message

		dealing_outlook.reply_email(
			"test@example.com", "Inbox", "Test Message", "Reply content", auto_send_email=True
		)

		reply_mock.Send.assert_called_once()
		reply_mock.Display.assert_not_called()

	def test_reply_email_no_match(
		self,
		dealing_outlook: Any,  # noqa ANN401: typing.Any not allowed
		outlook_mock: MagicMock,
		namespace_mock: MagicMock,
		folder_mock: MagicMock,
	) -> None:
		"""Test reply when no matching email is found.

		Verifies
		--------
		- No reply is created when no match found
		- No exceptions are raised

		Parameters
		----------
		dealing_outlook : Any
			DealingOutlook instance
		outlook_mock : MagicMock
			Mocked outlook object
		namespace_mock : MagicMock
			Mocked namespace object
		folder_mock : MagicMock
			Mocked folder object

		Returns
		-------
		None
		"""
		# Setup mocks
		outlook_mock.GetNamespace.return_value = namespace_mock
		account_folder = MagicMock()
		namespace_mock.Folders.__getitem__.return_value = account_folder
		account_folder.Folders.__getitem__.return_value = folder_mock

		message = MagicMock()
		message.Subject = "Other Message"
		folder_mock.Items.Count = 1
		folder_mock.Items.__getitem__.return_value = message

		dealing_outlook.reply_email("test@example.com", "Inbox", "Test Message", "Reply content")

		message.Reply.assert_not_called()
