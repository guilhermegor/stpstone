### API ANBIMA- MÉTODO POST PARA REALIZAÇÃO DE REQUISIÇÕES ###

import ast
import json

import requests

from stpstone.cals.handling_dates import DatasBR
from stpstone.handling_data.json import JsonFiles
from stpstone.handling_data.str import StrHandler
from stpstone.setting._global_slots import YAML_ANBIMA


class AnbimaDev:
    """
    REFENCE PRODUTCS AND INDEXES: REFERENCES: https://developers.anbima.com.br/api-portal/pt-br/content/swagger-preços-e-índices#/
    REFERENCES FUNDS: https://developers.anbima.com.br/api-portal/pt-br/content/swagger-fundos#/Fundos/findByCodigoFundoUsingGET
    """

    def access_token(self, client_id, client_secret):
        """
        REFERENCES: https://developers.anbima.com.br/api-portal/pt-br
        DOCSTRING: TOKEN PARA CONSULTA DA API DA ANBIMA
        INPUTS: CLIET_ID E CLIENT_SECRET
        OUTPUTS: JSON
        """
        base64_credentials = StrHandler().base64_encode(
            client_id, client_secret
        )
        url = 'https://api.anbima.com.br/oauth/access-token'
        headers = {
            'Content-Type': 'application/json',
            'Authorization': base64_credentials,
        }
        payload = {'grant_type': 'client_credentials'}
        response = requests.request(
            method='POST', url=url, headers=headers, data=json.dumps(payload)
        )
        jsonify_message = ast.literal_eval(
            StrHandler().find_between(
                str(response.text.encode('utf8')), "b'", "'"
            )
        )
        return JsonFiles().send_json(jsonify_message)

    def response_requests(self, url, headers, payload):
        """
        DOCSTRING: RESPOSTA PADRÃO PARA REQUISIÇÕES NO SWAGGER DO ANBIMA DEV
        INPUTS: URL, HEADERS E PAYLOAD
        OUTPUTS: DICT COM CÓDIGO DE RESPOSTA, MENSAGEM E URL
        """
        response = requests.request(
            method='POST', url=url, headers=headers, params=payload
        )
        try:
            jsonify_message = ast.literal_eval(
                StrHandler().find_between(
                    str(response.text.encode('utf8')), "b'", "'"
                )
            )
            dict_message = {
                'code': response,
                'message': jsonify_message,
                'url': response.url,
            }
        except:
            str_message = StrHandler().find_between(
                str(response.text.encode('utf8')), "b'", "'"
            )
            dict_message = {
                'code': response,
                'message': str_message,
                'url': response.url,
            }
        return dict_message

    def payload_consulta(
        self,
        combinacao_keys,
        data_interesse=DatasBR().sub_working_days(DatasBR().curr_date(), 1),
        page_int=0,
        size_int=1000,
        data_inicio=DatasBR().sub_working_days(DatasBR().curr_date(), 2),
        data_fim=DatasBR().sub_working_days(DatasBR().curr_date(), 1),
    ):
        """
        REFERENCES: https://developers.anbima.com.br/api-portal/pt-br/content/swagger-preços-e-índices#/
        DOCSTRING: CRIAÇÃO DE PARÂMETROS PARA SERVIREM COMO QUERIES PARA DIRECIONAR CONSULTA DO
            SERVIÇO
        INPUTS: COMBINAÇÃO KEYS (DATA, MES-ANO, PAGE-SIZE), DATA INTERESSE (DEFAULT D-1 ÚTIL),
            PAGE INTEGER (DEFAULT 0), SIZE INTEGER (DEFAULT 1000)
        OUTPUTS: JSON
        """
        if str(type(data_interesse)) == "<class 'datetime.date'>":
            data_interesse = data_interesse.strftime('%Y-%m-%d')
        if combinacao_keys == 'data':
            return {'data': data_interesse}
        elif combinacao_keys == 'mes-ano':
            return {'mes': data_interesse.month, 'ano': data_interesse.year}
        elif combinacao_keys == 'page-size':
            return {'page': page_int, 'size': size_int}
        elif combinacao_keys == 'data-inicio-fim':
            return {'data-inicio': data_inicio, 'data-fim': data_fim}
        else:
            raise Exception(
                'Nenhuma combinação de keys para composição de query válida'
            )

    def app_response(
        self,
        client_id,
        access_token,
        endpoint_correte,
        natureza_ativo,
        app_interesse,
        payload_query,
    ):
        """
        REFERENCES: https://developers.anbima.com.br/api-portal/pt-br/content/swagger-preços-e-índices#/Títulos%20Públicos/getUsingGET_34
        DOCSTRING: PROJEÇÕES PARA IGPM E IPCA
        INPUTS: CLIENT ID, ACCESS_TOKEN (RESULTADO DE UMA CONEXÃO NO OAUTH2), ENDPOINT CORRENTE
            (SANDBOX OU PRODUCAO), NATUREZA DO ATIVO (debentures, indices, indices+,
            titulos-publicos, cri-cra, fundos), APP DE INTERESSE URL (CONFERIR OS APPS DISPONÍVEIS
            NA REFERÊNCIA), PAYLOAD QUERY, APPS DE INTERESSE DA ANBIMA (POR PADRÃO UM
            DICIONÁRIO COM TODOS OS APPS DISPONIBILIZADOS EM 22/09/2020), E ENPOINTS
            (POR PADRÃO UM DICIONÁRIO
            COM TODOS OS ENDPOINTS DISPONÍVEIS EM 22/09/2020)
        TIPS: PARA SABER MAIS SOBRE AS KEYS DE APPS ANBIMA DEV E ENDPOINTS ANBIMA DEV POR FAVOR
            USAR O MÉTODO KEYS() E USAR O PAYLOAD CONSULTA (FUNÇÃO DA CLASSE) PARA COLHER O
            PAYLOAD DE INTERESSE
        OUTPUTS: JSON
        """
        url = (
            YAML_ANBIMA['hosts_anbima_dev_fundos'][endpoint_correte]
            + YAML_ANBIMA['apps_anbima_dev'][natureza_ativo][app_interesse]
        )
        headers = {
            'accept': 'application/json',
            'client_id': client_id,
            'access_token': access_token,
        }
        payload = payload_query
        return AnbimaDev().response_requests(url, headers, payload)


# client_id = CREDENTIALS_ANBIMA['client_id']
# password_id = CREDENTIALS_ANBIMA['client_secret']
# access_token = AnbimaDev().access_token(client_id, password_id)['access_token']
# print(access_token)
# print(AnbimaDev().app_response(client_id, access_token,
#                                'sandbox', 'titulos-publicos', 'vna',
#                                AnbimaDev().payload_consulta('data')))
