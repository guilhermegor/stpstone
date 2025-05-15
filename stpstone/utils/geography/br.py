### GROGRAPHICAL INFO WORLWIDE ###

from requests import request
import ast
import sys
sys.path.append(r'C:\Users\Guilherme\OneDrive\Dev\Python\Packages')
from stpstone.utils.parsers.str import StrHandler


class BrazilGeo:

    def states(self, bol_accents=True, key_nome='nome'):
        """
        DOCSTRING: RETURN BRAZILLIAN STATES
        INPUTS: BOOLEAN ACCENTS (WHETER SHOULD BE INCLUDED IN FEDERATIVE UNITY OR NOT,
            BY DEFAULT TRUE)
        OUTPUTS: SERIALIZED JSON WITH STATES ID, SHORT NAME AND REGION
        """
        # fetching states of brazillian territory
        url_localidades_brasil = 'https://servicodados.ibge.gov.br/api/v1/localidades/estados'
        # rest consult to ibge database
        response = request('GET', url_localidades_brasil)
        dict_message = ast.literal_eval(StrHandler().get_between(
            str(response.text.encode('utf8')), "b'", "'"))
        # latin characters to brazillian states names
        # wheter remove or not accents according to user will
        for dict_ativo in dict_message:
            dict_ativo[key_nome] = StrHandler().latin_characters(
                dict_ativo[key_nome])
            if bol_accents == False:
                dict_ativo[key_nome] = StrHandler(
                ).removing_accents(dict_ativo[key_nome])
        # sending response
        return dict_message

    def zip_code(self, list_zip_codes, url='https://cep.awesomeapi.com.br/json/{}',
                 method='GET', key_address_type='adress_type',
                 key_address='address', key_state='state'):
        """
        DOCSTRING: ZIP CODE LOCATION INFO
        INPUTS: LIST ZIP CODES, URL (DEFAULT), METHOD (DEFAULT)
        OUTPUTS: DICTIONARY
        """
        # setting variables
        dict_zip_adresses = dict()
        # looping through each zip code, requesting info about adrees, and raise exception
        #   if status code is different from 2xx
        for zip_ in list_zip_codes:
            resp_req = request(method=method, url=url.format(zip_))
            #   raises exception when not a 2xx response
            resp_req.raise_for_status()
            #   generating json
            json_zip_codes = resp_req.json()
            #   organizing data to export format
            dict_zip_adresses[zip_] = [json_zip_codes[key_address_type],
                                       json_zip_codes[key_address], json_zip_codes[key_state]]
        # return data with zip code location info
        return dict_zip_adresses


# print(BrazilGeo().states(bol_accents=False))
# # output
# [{'id': 11, 'sigla': 'RO', 'nome': 'Rondonia', 'regiao': {'id': 1, 'sigla': 'N', 'nome': 'Norte'}}, {'id': 12, 'sigla': 'AC', 'nome': 'Acre', 'regiao': {'id': 1, 'sigla': 'N', 'nome': 'Norte'}}, {'id': 13, 'sigla': 'AM', 'nome': 'Amazonas', 'regiao': {'id': 1, 'sigla': 'N', 'nome': 'Norte'}}, {'id': 14, 'sigla': 'RR', 'nome': 'Roraima', 'regiao': {'id': 1, 'sigla': 'N', 'nome': 'Norte'}}, {'id': 15, 'sigla': 'PA', 'nome': 'Para', 'regiao': {'id': 1, 'sigla': 'N', 'nome': 'Norte'}}, {'id': 16, 'sigla': 'AP', 'nome': 'Amapa', 'regiao': {'id': 1, 'sigla': 'N', 'nome': 'Norte'}}, {'id': 17, 'sigla': 'TO', 'nome': 'Tocantins', 'regiao': {'id': 1, 'sigla': 'N', 'nome': 'Norte'}}, {'id': 21, 'sigla': 'MA', 'nome': 'Maranhao', 'regiao': {'id': 2, 'sigla': 'NE', 'nome': 'Nordeste'}}, {'id': 22, 'sigla': 'PI', 'nome': 'Piaui', 'regiao': {'id': 2, 'sigla': 'NE', 'nome': 'Nordeste'}}, {'id': 23, 'sigla': 'CE', 'nome': 'Ceara', 'regiao': {'id': 2, 'sigla': 'NE', 'nome': 'Nordeste'}}, {'id': 24, 'sigla': 'RN', 'nome': 'Rio Grande do Norte', 'regiao': {'id': 2, 'sigla': 'NE', 'nome': 'Nordeste'}}, {'id': 25, 'sigla': 'PB', 'nome': 'Paraiba', 'regiao': {'id': 2, 'sigla': 'NE', 'nome': 'Nordeste'}}, {'id': 26, 'sigla': 'PE', 'nome': 'Pernambuco', 'regiao': {'id': 2, 'sigla': 'NE', 'nome': 'Nordeste'}}, {'id': 27, 'sigla': 'AL', 'nome': 'Alagoas', 'regiao': {'id': 2, 'sigla': 'NE', 'nome': 'Nordeste'}}, {'id': 28, 'sigla': 'SE', 'nome': 'Sergipe', 'regiao': {'id': 2, 'sigla': 'NE', 'nome': 'Nordeste'}}, {'id': 29, 'sigla': 'BA', 'nome': 'Bahia', 'regiao': {'id': 2, 'sigla': 'NE', 'nome': 'Nordeste'}}, {'id': 31, 'sigla': 'MG', 'nome': 'Minas Gerais', 'regiao': {'id': 3, 'sigla': 'SE', 'nome': 'Sudeste'}}, {'id': 32, 'sigla': 'ES', 'nome': 'Espirito Santo', 'regiao': {'id': 3, 'sigla': 'SE', 'nome': 'Sudeste'}}, {'id': 33, 'sigla': 'RJ', 'nome': 'Rio de Janeiro', 'regiao': {'id': 3, 'sigla': 'SE', 'nome': 'Sudeste'}}, {'id': 35, 'sigla': 'SP', 'nome': 'Sao Paulo', 'regiao': {'id': 3, 'sigla': 'SE', 'nome': 'Sudeste'}}, {'id': 41, 'sigla': 'PR', 'nome': 'Parana', 'regiao': {'id': 4, 'sigla': 'S', 'nome': 'Sul'}}, {'id': 42, 'sigla': 'SC', 'nome': 'Santa Catarina', 'regiao': {'id': 4, 'sigla': 'S', 'nome': 'Sul'}}, {'id': 43, 'sigla': 'RS', 'nome': 'Rio Grande do Sul', 'regiao': {'id': 4, 'sigla': 'S', 'nome': 'Sul'}}, {'id': 50, 'sigla': 'MS', 'nome': 'Mato Grosso do Sul', 'regiao': {'id': 5, 'sigla': 'CO', 'nome': 'Centro-Oeste'}}, {'id': 51, 'sigla': 'MT', 'nome': 'Mato Grosso', 'regiao': {'id': 5, 'sigla': 'CO', 'nome': 'Centro-Oeste'}}, {'id': 52, 'sigla': 'GO', 'nome': 'Goias', 'regiao': {'id': 5, 'sigla': 'CO', 'nome': 'Centro-Oeste'}}, {'id': 53, 'sigla': 'DF', 'nome': 'Distrito Federal', 'regiao': {'id': 5, 'sigla': 'CO', 'nome': 'Centro-Oeste'}}]
