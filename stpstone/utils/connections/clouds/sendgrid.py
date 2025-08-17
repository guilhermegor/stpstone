"""SendGrid email integration module.

This module provides a class for sending emails through SendGrid's API,
handling email composition and delivery with proper error handling.
"""

from typing import Optional

from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker


class SendGrid(metaclass=TypeChecker):
    """Class for handling email sending through SendGrid API."""

    def _validate_email_params(
        self,
        str_sender: str,
        list_recipients: list[str],
        subject: str,
        html_body: str,
        token: str
    ) -> None:
        """Validate email parameters before sending.

        Parameters
        ----------
        str_sender : str
            Email sender address
        list_recipients : list[str]
            list of recipient email addresses
        subject : str
            Email subject line
        html_body : str
            HTML content of the email
        token : str
            SendGrid API token

        Raises
        ------
        ValueError
            If sender email, recipients list, subject, HTML body, or API token is empty
        """
        if not str_sender:
            raise ValueError("Sender email cannot be empty")
        if not list_recipients:
            raise ValueError("Recipients list cannot be empty")
        if not subject:
            raise ValueError("Email subject cannot be empty")
        if not html_body:
            raise ValueError("HTML body cannot be empty")
        if not token:
            raise ValueError("API token cannot be empty")

    def send_email(
        self,
        str_sender: str,
        list_recipients: list[str],
        list_cc: Optional[list[str]],
        subject: str,
        html_body: str,
        token: str
    ) -> bool:
        """Send email through SendGrid API.

        Parameters
        ----------
        str_sender : str
            Email sender address
        list_recipients : list[str]
            list of recipient email addresses
        list_cc : Optional[list[str]]
            list of CC recipient email addresses (optional)
        subject : str
            Email subject line
        html_body : str
            HTML content of the email (should start and end with double quotes,
            using single quotes inside)
        token : str
            SendGrid API token

        Returns
        -------
        bool
            True if email was sent successfully, False otherwise

        Raises
        ------
        ValueError
            Captures any given error beside _validate_email_params
        """
        self._validate_email_params(str_sender, list_recipients, subject, html_body, token)

        message = Mail(
            from_email=str_sender,
            to_emails=list_recipients,
            subject=subject,
            html_content=html_body
        )
        
        if list_cc:
            for cc_email in list_cc:
                message.add_cc(cc_email)

        try:
            sg = SendGridAPIClient(token)
            response = sg.send(message)
            return response.status_code == 202
        except Exception as err:
            raise ValueError(f"Error sending email from sendgrid: {str(err)}") from err