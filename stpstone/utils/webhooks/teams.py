"""Microsoft Teams webhook integration for automated messaging.

This module provides a class for sending messages to Microsoft Teams channels
using webhooks with the pymsteams library.
"""

import pymsteams

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker


class WebhookTeams(metaclass=TypeChecker):
	"""Class for handling Microsoft Teams webhook messaging."""

	def _validate_url(self, url_webhook: str) -> None:
		"""Validate the webhook URL format.

		Parameters
		----------
		url_webhook : str
			Teams webhook URL to validate

		Raises
		------
		ValueError
			If URL is empty
			If URL is not a string
			If URL does not start with https://
		"""
		if not url_webhook:
			raise ValueError("Webhook URL cannot be empty")
		if not url_webhook.startswith("https://"):
			raise ValueError("Webhook URL must start with https://")

	def __init__(self, url_webhook: str) -> None:
		"""Initialize WebhookTeams instance.

		Parameters
		----------
		url_webhook : str
			Microsoft Teams webhook URL
		"""
		self._validate_url(url_webhook)
		self.url_webhook = url_webhook

	def send_message(
		self, str_msg: str, str_title: str = "ROUTINE_CONCLUSION", bool_print_message: bool = False
	) -> None:
		"""Send plain message with text and title to Teams channel.

		Parameters
		----------
		str_msg : str
			Message content to send
		str_title : str
			Message title (default: "ROUTINE_CONCLUSION")
		bool_print_message : bool
			Whether to print message to console (default: False)

		Raises
		------
		ValueError
			If message is empty
			If message is not a string
		"""
		if not str_msg:
			raise ValueError("Message cannot be empty")

		try:
			teams_message = pymsteams.connectorcard(self.url_webhook)
			teams_message.title(str_title)
			teams_message.text(str_msg)
			if bool_print_message:
				teams_message.printme()
			teams_message.send()
		except Exception as err:
			raise ValueError(f"Failed to send Teams message: {str(err)}") from err
