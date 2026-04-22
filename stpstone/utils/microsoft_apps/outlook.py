"""Outlook email client interaction utilities.

This module provides a class for sending, receiving, and managing emails through Microsoft Outlook
using the win32com library. It includes functionality for sending emails with attachments,
downloading attachments, retrieving email content, and automated replies.
"""

import platform


if platform.system() != "Windows":
	raise OSError("This module requires a Windows operating system to function properly.")

import os
from typing import Literal, Optional, TypedDict, Union

import win32com.client as win32

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker
from stpstone.utils.parsers.folders import DirFilesManagement
from stpstone.utils.parsers.str import StrHandler


class ReturnDownloadAttch(TypedDict):
	"""Typed dictionary for download_attch method return value.

	Parameters
	----------
	file_path : bool
		Status of file download (True if successful)
	"""

	file_path: bool


class DealingOutlook(metaclass=TypeChecker):
	"""Class for handling Outlook email operations."""

	def _validate_email_account(self, email_account: str) -> None:
		"""Validate email account format.

		Parameters
		----------
		email_account : str
			Email account to validate

		Returns
		-------
		None

		Raises
		------
		ValueError
			If email account is empty
			If email account does not contain '@' character
		"""
		if not email_account:
			raise ValueError("Email account cannot be empty")
		if "@" not in email_account:
			raise ValueError("Email account must contain '@' character")

	def _validate_folder_path(self, folder_path: str) -> None:
		"""Validate Outlook folder path.

		Parameters
		----------
		folder_path : str
			Folder path to validate

		Returns
		-------
		None

		Raises
		------
		ValueError
			If folder path is empty
		"""
		if not folder_path:
			raise ValueError("Folder path cannot be empty")

	def _validate_attachment_path(self, path: Union[str, list[str]]) -> None:
		"""Validate attachment path(s).

		Parameters
		----------
		path : Union[str, list[str]]
			Attachment path(s) to validate

		Returns
		-------
		None

		Raises
		------
		ValueError
			If path is not a string or list
			If any path in list is not a string
		"""
		if not isinstance(path, (str, list)):
			raise ValueError("Attachment path must be string or list of strings")
		if isinstance(path, list) and not all(isinstance(p, str) for p in path):
			raise ValueError("All attachment paths in list must be strings")

	def send_email(
		self,
		mail_subject: str,
		mail_to: Optional[str] = None,
		mail_cc: Optional[str] = None,
		mail_bcc: Optional[str] = None,
		mail_body: Optional[str] = None,
		mail_attachments: Optional[list[str]] = None,
		send_behalf_of: Optional[str] = None,
		auto_send_email: bool = False,
		bool_frame_sender: bool = False,
		bool_read_receipt: bool = False,
		bool_delivery_receipt: bool = False,
		bool_image_display: bool = False,
		str_html_signature: Optional[str] = None,
	) -> None:
		"""Send email with attachments and optional send-on-behalf functionality.

		Parameters
		----------
		mail_subject : str
			Email subject line
		mail_to : Optional[str]
			Primary recipient email address (default: None)
		mail_cc : Optional[str]
			CC recipient email address (default: None)
		mail_bcc : Optional[str]
			BCC recipient email address (default: None)
		mail_body : Optional[str]
			Email body content (default: None)
		mail_attachments : Optional[list[str]]
			list of file paths to attach (default: None)
		send_behalf_of : Optional[str]
			Email address to send on behalf of (default: None)
		auto_send_email : bool
			Whether to send automatically without display (default: False)
		bool_frame_sender : bool
			Whether to frame sender account (default: False)
		bool_read_receipt : bool
			Whether to request read receipt (default: False)
		bool_delivery_receipt : bool
			Whether to request delivery receipt (default: False)
		bool_image_display : bool
			Whether to display images (default: False)
		str_html_signature : Optional[str]
			HTML signature to append (default: None)

		Returns
		-------
		None

		Raises
		------
		RuntimeError
			If send fails

		References
		----------
		.. [1] https://stackoverflow.com/questions/63435690/send-outlook-email-with-attachment-to-list-of-users-in-excel-with-python
		"""
		outlook = win32.Dispatch("outlook.application")
		mail = outlook.CreateItem(0)

		if mail_to:
			mail.To = mail_to
		if mail_cc:
			mail.CC = mail_cc
		if mail_bcc:
			mail.BCC = mail_bcc

		mail.Subject = mail_subject

		if send_behalf_of:
			if not bool_frame_sender:
				for account in outlook.Session.Accounts:
					if str(account) == send_behalf_of:
						send_behalf_of_account = account
						break
				mail._oleobj_.Invoke(*(64209, 0, 8, 0, send_behalf_of_account))
			mail.SentOnBehalfOfName = send_behalf_of

		if mail_body is not None:
			mail.HTMLBody = mail_body
			if str_html_signature is not None:
				mail.HTMLBody += str_html_signature

		if mail_attachments is not None:
			for attachment in mail_attachments:
				if os.path.exists(attachment):
					mail.Attachments.Add(Source=attachment)

		mail.ReadReceiptRequested = bool_read_receipt
		mail.OriginatorDeliveryReportRequested = bool_delivery_receipt

		if not auto_send_email:
			mail.Display()
		elif auto_send_email:
			try:
				if bool_image_display:
					mail.Display()
				mail.Send()
			except Exception as err:
				raise RuntimeError(f"Failed to send email: {str(err)}") from err
		else:
			mail.Close()

	def download_attch(
		self,
		email_account: str,
		outlook_folder: str,
		subj_sub_string: str,
		attch_save_path: Union[str, list[str]],
		bool_save_file_w_original_name: bool = False,
		list_fileformat: Optional[list[str]] = None,
		outlook_subfolder: Optional[str] = None,
		move_to_folder: Optional[str] = None,
		save_only_first_event: bool = False,
		bool_leave_after_first_occurance: bool = False,
		bool_break_loops: bool = False,
	) -> ReturnDownloadAttch:
		"""Download attachments from emails matching subject criteria.

		Parameters
		----------
		email_account : str
			Outlook email account name
		outlook_folder : str
			Main Outlook folder to search
		subj_sub_string : str
			Subject substring to match
		attch_save_path : Union[str, list[str]]
			Path(s) to save attachments
		bool_save_file_w_original_name : bool
			Save with original filename (default: False)
		list_fileformat : Optional[list[str]]
			list of allowed file formats (default: None)
		outlook_subfolder : Optional[str]
			Subfolder to search (default: None)
		move_to_folder : Optional[str]
			Folder to move emails to (default: None)
		save_only_first_event : bool
			Save only first matching email (default: False)
		bool_leave_after_first_occurance : bool
			Stop after first match (default: False)
		bool_break_loops : bool
			Break loops early (default: False)

		Returns
		-------
		ReturnDownloadAttch
			Dictionary with file paths and download status
		"""
		self._validate_email_account(email_account)
		self._validate_folder_path(outlook_folder)
		self._validate_attachment_path(attch_save_path)

		out_app = win32.Dispatch("Outlook.Application")
		out_namespace = out_app.GetNamespace("MAPI")

		if outlook_subfolder:
			out_iter_folder = (
				out_namespace.Folders[email_account]
				.Folders[outlook_folder]
				.Folders[outlook_subfolder]
			)
		else:
			out_iter_folder = out_namespace.Folders[email_account].Folders[outlook_folder]

		if move_to_folder:
			out_move_to_folder = out_namespace.Folders[email_account].Folders[move_to_folder]

		dict_attch_saving_status: ReturnDownloadAttch = {}

		if isinstance(attch_save_path, str):
			list_attch_save_path = [attch_save_path]
		else:
			list_attch_save_path = attch_save_path.copy()

		item_count = out_iter_folder.Items.Count
		if item_count == 0:
			return dict_attch_saving_status

		for i in range(item_count):
			if bool_break_loops:
				break

			message = out_iter_folder.Items[i]
			if not StrHandler().find_substr_str(message.Subject, subj_sub_string):
				continue

			for path in list_attch_save_path:
				if bool_break_loops:
					break

				for attach in message.Attachments:
					if (
						bool_save_file_w_original_name
						and list_fileformat
						and DirFilesManagement().get_file_format_from_file_name(attach.FileName)
						in list_fileformat
					):
						full_path = os.path.join(path, attach.FileName)
						attach.SaveAsFile(full_path)
						dict_attch_saving_status[full_path] = DirFilesManagement().object_exists(
							full_path
						)
					elif not bool_save_file_w_original_name:
						attach.SaveAsFile(path)
						dict_attch_saving_status[path] = DirFilesManagement().object_exists(path)

					if move_to_folder:
						message.Move(out_move_to_folder)

					if save_only_first_event and dict_attch_saving_status:
						return dict_attch_saving_status

				if bool_leave_after_first_occurance:
					bool_break_loops = True

		return dict_attch_saving_status

	def received_email_subject_w_rule(
		self,
		email_account: str,
		outlook_folder: str,
		subj_sub_string: str,
		outlook_subfolder: Optional[str] = None,
		output: Literal["subject", "message_raw", "properties"] = "subject",
	) -> Union[list[str], list[object], list[dict[str, object]]]:
		"""Retrieve emails matching subject criteria with various output formats.

		Parameters
		----------
		email_account : str
			Outlook email account name
		outlook_folder : str
			Main Outlook folder to search
		subj_sub_string : str
			Subject substring to match
		outlook_subfolder : Optional[str]
			Subfolder to search (default: None)
		output : Literal['subject', 'message_raw', 'properties']
			Output format type (default: "subject")

		Returns
		-------
		Union[list[str], list[object], list[dict[str, object]]]
			list of results based on output format

		Raises
		------
		NameError
			If output format is invalid

		References
		----------
		.. [1] https://stackoverflow.com/questions/22813814/clearly-documented-reading-of-emails-functionality-with-python-win32com-outlook
		.. [2] https://learn.microsoft.com/en-us/dotnet/api/microsoft.office.interop.outlook.mailitem?view=outlook-pia&redirectedfrom=MSDN
		"""
		self._validate_email_account(email_account)
		self._validate_folder_path(outlook_folder)

		out_app = win32.Dispatch("Outlook.Application")
		out_namespace = out_app.GetNamespace("MAPI")

		if outlook_subfolder:
			out_iter_folder = (
				out_namespace.Folders[email_account]
				.Folders[outlook_folder]
				.Folders[outlook_subfolder]
			)
		else:
			out_iter_folder = out_namespace.Folders[email_account].Folders[outlook_folder]

		item_count = out_iter_folder.Items.Count
		list_emails_subjects = []

		if item_count == 0:
			return list_emails_subjects

		for i in range(item_count):
			message = out_iter_folder.Items[i]
			if not StrHandler().find_substr_str(message.Subject, subj_sub_string):
				continue

			if output == "subject":
				list_emails_subjects.append(message.Subject)
			elif output == "message_raw":
				list_emails_subjects.append(message)
			elif output == "properties":
				list_emails_subjects.append(
					{
						"subject": message.Subject,
						"last_edition": message.LastModificationTime,
						"creation_time": message.CreationTime,
					}
				)
			else:
				raise NameError(f"Output format '{output}' is invalid")

		return list_emails_subjects

	def get_body_content(
		self,
		email_account: str,
		outlook_folder: str,
		subj_sub_string: str,
		outlook_subfolder: Optional[str] = None,
	) -> list[dict[str, object]]:
		"""Retrieve email body content for messages matching subject criteria.

		Parameters
		----------
		email_account : str
			Outlook email account name
		outlook_folder : str
			Main Outlook folder to search
		subj_sub_string : str
			Subject substring to match
		outlook_subfolder : Optional[str]
			Subfolder to search (default: None)

		Returns
		-------
		list[dict[str, object]]
			list of dictionaries containing email metadata and body content

		References
		----------
		.. [1] https://stackoverflow.com/questions/22813814/clearly-documented-reading-of-emails-functionality-with-python-win32com-outlook
		.. [2] https://learn.microsoft.com/en-us/dotnet/api/microsoft.office.interop.outlook.mailitem?view=outlook-pia&redirectedfrom=MSDN
		"""
		self._validate_email_account(email_account)
		self._validate_folder_path(outlook_folder)

		out_app = win32.Dispatch("Outlook.Application")
		out_namespace = out_app.GetNamespace("MAPI")

		if outlook_subfolder:
			out_iter_folder = (
				out_namespace.Folders[email_account]
				.Folders[outlook_folder]
				.Folders[outlook_subfolder]
			)
		else:
			out_iter_folder = out_namespace.Folders[email_account].Folders[outlook_folder]

		item_count = out_iter_folder.Items.Count
		list_emails_content = []

		if item_count == 0:
			return list_emails_content

		for i in range(item_count):
			message = out_iter_folder.Items[i]
			if StrHandler().find_substr_str(message.Subject, subj_sub_string):
				list_emails_content.append(
					{
						"subject": message.Subject,
						"last_edition": message.LastModificationTime,
						"creation_time": message.CreationTime,
						"body": message.body,
					}
				)

		return list_emails_content

	def reply_email(
		self,
		email_account: str,
		outlook_folder: str,
		subj_sub_string: str,
		msg_body: str,
		mail_cc: Optional[str] = None,
		mail_bcc: Optional[str] = None,
		auto_send_email: bool = False,
		bool_image_display: bool = False,
		outlook_subfolder: Optional[str] = None,
	) -> None:
		"""Send automated reply to emails matching subject criteria.

		Parameters
		----------
		email_account : str
			Outlook email account name
		outlook_folder : str
			Main Outlook folder to search
		subj_sub_string : str
			Subject substring to match
		msg_body : str
			Reply message body
		mail_cc : Optional[str]
			CC recipient email address (default: None)
		mail_bcc : Optional[str]
			BCC recipient email address (default: None)
		auto_send_email : bool
			Whether to send automatically (default: False)
		bool_image_display : bool
			Whether to display images (default: False)
		outlook_subfolder : Optional[str]
			Subfolder to search (default: None)

		Returns
		-------
		None

		Raises
		------
		RuntimeError
			If no matching email is found
		"""
		self._validate_email_account(email_account)
		self._validate_folder_path(outlook_folder)

		out_app = win32.Dispatch("Outlook.Application")
		out_namespace = out_app.GetNamespace("MAPI")

		if outlook_subfolder:
			out_iter_folder = (
				out_namespace.Folders[email_account]
				.Folders[outlook_folder]
				.Folders[outlook_subfolder]
			)
		else:
			out_iter_folder = out_namespace.Folders[email_account].Folders[outlook_folder]

		item_count = out_iter_folder.Items.Count
		if item_count == 0:
			return

		reply = None
		for i in range(item_count):
			message = out_iter_folder.Items[i]
			if message.Subject == subj_sub_string:
				reply = message.Reply()
				reply.HTMLBody += msg_body
				break

		if not reply:
			return

		if mail_cc:
			reply.CC = mail_cc
		if mail_bcc:
			reply.BCC = mail_bcc

		if not auto_send_email:
			reply.Display()
		else:
			try:
				if bool_image_display:
					reply.Display()
				reply.Send()
			except Exception as err:
				raise RuntimeError(f"Failed to send reply: {str(err)}") from err
