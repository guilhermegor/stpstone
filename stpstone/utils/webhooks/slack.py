"""Slack webhook integration for sending messages.

This module provides a class for sending messages to Slack channels via webhooks,
with customizable username and emoji icons for the messages.
"""

from typing import Literal, Union

from requests import request

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker
from stpstone.utils.parsers.json import JsonFiles


class WebhookSlack(metaclass=TypeChecker):
    """Class for handling Slack webhook message sending operations."""

    def _validate_url(self, url: str) -> None:
        """Validate Slack webhook URL format.

        Parameters
        ----------
        url : str
            Webhook URL to validate

        Raises
        ------
        ValueError
            If URL is empty
            If URL is not a string
            If URL does not start with https://hooks.slack.com/services/
        """
        if not url:
            raise ValueError("Webhook URL cannot be empty")
        if not url.startswith("https://hooks.slack.com/services/"):
            raise ValueError("Invalid Slack webhook URL format")

    def _validate_channel_id(self, channel_id: str) -> None:
        """Validate Slack channel ID format.

        Parameters
        ----------
        channel_id : str
            Channel ID to validate

        Raises
        ------
        ValueError
            If channel ID is empty
            If channel ID is not a string
            If channel ID does not start with # or @
        """
        if not channel_id:
            raise ValueError("Channel ID cannot be empty")
        if not (channel_id.startswith("#") or channel_id.startswith("@")):
            raise ValueError("Channel ID must start with # or @")

    def __init__(
        self,
        url_webhook: str,
        id_channel: str,
        str_username: str = "webhookbot",
        str_icon_emoji: str = ":bricks:"
    ) -> None:
        """Initialize WebhookSlack instance.

        Parameters
        ----------
        url_webhook : str
            Slack webhook URL
        id_channel : str
            Slack channel ID (starting with # or @)
        str_username : str
            Bot username (default: "webhookbot")
        str_icon_emoji : str
            Emoji icon for messages (default: ":bricks:")
        """
        self._validate_url(url_webhook)
        self._validate_channel_id(id_channel)
        
        self.url_webhook = url_webhook
        self.id_channel = id_channel
        self.str_username = str_username
        self.str_icon_emoji = str_icon_emoji

    def send_message(
        self,
        str_msg: str,
        str_method: Literal["POST", "GET", "PUT", "DELETE"] = "POST",
        timeout: Union[tuple[float, float], float, int] = 10
    ) -> str:
        """Send message to Slack channel via webhook.

        Parameters
        ----------
        str_msg : str
            Message text to send
        str_method : Literal['POST', 'GET', 'PUT', 'DELETE']
            HTTP method to use (default: "POST")
        timeout : Union[tuple[float, float], float, int]
            Request timeout in seconds (default: 10)

        Returns
        -------
        str
            Response text from Slack API

        Raises
        ------
        ValueError
            If message is empty
            If HTTP request fails

        References
        ----------
        .. [1] https://api.slack.com/messaging/webhooks
        .. [2] https://raw.githubusercontent.com/iamcal/emoji-data/master/emoji.json
        """
        if not str_msg:
            raise ValueError("Message cannot be empty")

        dict_payload = {
            "channel": self.id_channel,
            "username": self.str_username,
            "text": str_msg,
            "icon_emoji": self.str_icon_emoji
        }
        dict_headers = {
            "Content-Type": "application/x-www-form-urlencoded"
        }

        try:
            resp_req = request(
                str_method,
                self.url_webhook,
                headers=dict_headers,
                data=JsonFiles().dict_to_json(dict_payload), 
                timeout=timeout
            )
            resp_req.raise_for_status()
            return resp_req.text
        except Exception as err:
            raise ValueError(f"Failed to send message: {str(err)}") from err