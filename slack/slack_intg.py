### MODULE TO INTERACTE WITH SLACK API ###

from slack import WebClient
from slack.errors import SlackApiError


class SlackAPI:

    def __init__(self, url_webhook, id_channel):
        self.url_webhook = url_webhook
        self.id_channel = id_channel

    @property
    def client(self):
        '''
        DOCSTRING: CLIENT OF WEBHOOK TO SLACK
        INPUTS: -
        OUTPUTS: CLIENT PROPERTIES
        '''
        return WebClient(token=self.url_webhook)

    def send_plain_message(self, message_str):
        '''
        DOCSTRING: SEND PLAIN MESSAGE TO SLACK CHANNEL
        INPUTS: TOKEN, CHANNEL ID AND MESSAGE
        OUTPUTS: STATUS OF ACCOMPLISHMENT
        '''
        # sending plain message
        try:
            response = SlackAPI(self.url_webhook, self.id_channel).client.chat_postMessage(
                channel=self.id_channel, text=message_str)
            return 'Status of accomplishmet: {}'.format(
                response['message']['text'] == message_str)
        except SlackApiError as err:
            assert err.response['ok'] is False
            # string like 'invalid_auth', 'channel_not_found'
            assert err.response['error']
            return 'Status of accomplishment: NOK. Request returned an error: {}'.format(
                err.response['error'])

    def upload_files(self, file_path):
        '''
        DOCSTRING: UPLOADING FILE TO A SLACK CHANNEL
        INPUTS: FILE PATH
        OUTPUTS: STATUS OF ACCOMPLISHMENT
        '''
        try:
            response = SlackAPI(self.url_webhook, self.id_channel).client.files_upload(
                channels=self.id_channel, file=file_path)
            return 'Status of accomplishmet: OK, uploaded file: {}'.format(response['file'])
        except SlackApiError as err:
            # string like 'invalid_auth', 'channel_not_found'
            assert err.response['error']
            return 'Status of accomplishment: NOK. Request returned an error: {}'.format(
                err.response['error'])
