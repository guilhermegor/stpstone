### ENVIO DE MENSAGENS AUTOMÁTICAS NO TEAMS ###

import pymsteams


class WebhookTeams:

    def __init__(self, url_webhook:str) -> None:
        self.url_webhook = url_webhook

    def send_message(self, str_msg:str, str_title:str='ROUTINE_CONCLUSION',
                           bl_print_message:bool=False) -> None:
        """
        DOCSTRING: SEND PLAIN MESSAGE WITH TEXT AND TITLE
        INPUTS: WEBHOOK CONNECTION, MESSAGE, TITLE AND BODY
        OUTPUTS: -
        """
        teams_message = pymsteams.connectorcard(self.url_webhook)
        teams_message.title(str_title)
        teams_message.text(str_msg)
        if bl_print_message == True:
            teams_message.printme()
        teams_message.send()
