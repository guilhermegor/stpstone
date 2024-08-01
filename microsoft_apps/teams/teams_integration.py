### ENVIO DE MENSAGENS AUTOMÁTICAS NO TEAMS ###

import pymsteams


class TeamsConn:

    def send_plain_message(self, webhook_url, mess_title, mess_text, bl_print_message=False):
        '''
        DOCSTRING: SEND PLAIN MESSAGE WITH TEXT AND TITLE
        INPUTS: WEBHOOK CONNECTION, MESSAGE, TITLE AND BODY
        OUTPUTS: TEAMS MESSAGE
        '''
        teams_message = pymsteams.connectorcard(webhook_url)
        teams_message.title(mess_title)
        teams_message.text(mess_text)
        if bl_print_message == True:
            teams_message.printme()
        teams_message.send()
