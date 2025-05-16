### WEBHOOK SLACK ###

from requests import request
from stpstone.utils.parsers.json import JsonFiles


class WebhookSlack:

    def __init__(self, url_webhook:str, id_channel:str, str_username:str='webhookbot',
                 str_icon_emoji:str=':bricks:') -> None:
        self.url_webhook = url_webhook
        self.id_channel = id_channel
        self.str_username = str_username
        self.str_icon_emoji = str_icon_emoji

    def send_message(self, str_msg:str, str_method:str='POST') -> None:
        """
        DOCSTRING:
        INPUTS:
            - MESSAGE:STR
            - METHOD:STR
            - ICON_EMOJI:STR (AVAILABLE EMOJIS: https://raw.githubusercontent.com/iamcal/emoji-data/master/emoji.json)
        OUTPUTS: STATUS OF ACCOMPLISHMENT
        """
        dict_payload = {
            'channel': self.id_channel,
            'username': self.str_username,
            'text': str_msg,
            'icon_emoji': self.str_icon_emoji
        }
        dict_headers = {
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        resp_req = request(str_method, self.url_webhook, headers=dict_headers,
                           data=JsonFiles().dict_to_json(dict_payload))
        resp_req.raise_for_status()
        return resp_req.text
