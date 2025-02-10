### MODULE TO HANDLE SENDGRID X PYTHON INTEGRATION ###

from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail


class SendGrid:

    def send_email(self, str_sender, list_recipients, list_cc, subject, html_body, token):
        """
        DOCSTRING: SEND EMAIL FROM SENDGRID
        INPUTS: SENDER STR, LIST OF RECIPIENTS STRINGS, SUBJECT, HTML BODY (STARTING AND ENDING WITH DOUBLE QUOTES, USING ONLY SINGLE QUOTES INSIDE IT) AND TOKEN
        OUPUTS: STATUS OF ACCOMPLISHMENT
        """
        message = Mail(
            str_sender=str_sender,
            to_emails=list_recipients,
            mail_cc=list_cc,
            subject=subject,
            html_content=html_body)
        try:
            sg = SendGridAPIClient(token)
            return sg.send(message)
        except Exception as e:
            raise Exception('Error sending email from sendgrid: {}'.format(e))
