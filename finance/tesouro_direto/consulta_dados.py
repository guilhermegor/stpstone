
### DADOS DE NEGOCIAÇÃO DE DÍVIDA PÚBLICA BRASILEIRA ###

import datetime
import pandas as pd
from io import BytesIO
from requests import request
import sys
sys.path.append(r'C:\Users\Guilherme\OneDrive\Dev\Python\Packages')
from stpstone.handling_data.str import StrHandler
from stpstone.central.global_slots import YAML_BR_TRS
from stpstone.cals.handling_dates import DatesBR


class ConsultaDadosTesouroDireto:

    def pu_taxa_abertura_contratos(self, contrato, data_mtm, data_vencimento):
        '''
        REFERENCES: https://sisweb.tesouro.gov.br/apex/f?p=2031:2:0:
        DOCSTRING: PU E TAXA NA ABERTURA DO DIA
        INPUTS: CONTRATO, DATA DE REFERÊNCIA PARA MARCAÇÃO À MERCADO E DATA DE VENCIMENTO
        OUTPUTS: DICIONÁRIO (DIA, TAXA DE COMPRA NA ABERTURA, TAXA DE VENDA NA ABERTURA, 
            PU COMPRA NA ABERTURA, PU VENDA NA ABERTURA, PU VENDA NA ABERTURA, PU BASE ABERTURA)
        '''
        # ajustando tipos da variáveis
        if isinstance(data_mtm, datetime.date) == True:
            data_mtm = DatesBR().datetime_to_string(data_mtm)
        if isinstance(data_vencimento, datetime.date) == True:
            data_vencimento = DatesBR().datetime_to_string(data_vencimento)
        # datas de referência para a solicitação
        data_vencimento = DatesBR().str_date_to_datetime(data_vencimento)
        ano_vencimento = DatesBR().str_date_to_datetime(data_mtm).strftime('%Y')
        data_vencimento = data_vencimento.strftime('%d%m%y')
        # requisição de tabela do tesouro direto com o contrato de interesse para o ano de
        #   mtm de referência
        url = 'https://sisweb.tesouro.gov.br/apex/cosis/sistd/obtem_arquivo/{}'.format(
            YAML_BR_TRS['sisweb_td_host_port'][contrato][ano_vencimento])
        response = request('GET', url).content
        # importando para dataframe o contrato com vencimento de interesse
        with BytesIO(response) as fh:
            df_contrato_vencimento = pd.io.excel.read_excel(
                fh, sheet_name='{} {}'.format(contrato, data_vencimento))
        new_header = df_contrato_vencimento.iloc[0]
        df_contrato_vencimento = df_contrato_vencimento[1:]
        df_contrato_vencimento.columns = new_header
        # coletando taxa e pu para a data de referência
        for index, row in df_contrato_vencimento.iterrows():
            if df_contrato_vencimento.loc[index, 'Dia'] == data_mtm:
                return {
                    'dia': df_contrato_vencimento.loc[index, 'Dia'],
                    'taxa_compra_abertura': df_contrato_vencimento.loc[index, 'Taxa Compra Manhã'],
                    'taxa_venda_abertura': df_contrato_vencimento.loc[index, 'Taxa Venda Manhã'],
                    'pu_compra_abertura': df_contrato_vencimento.loc[index, 'PU Compra Manhã'],
                    'pu_venda_abertura': df_contrato_vencimento.loc[index, 'PU Venda Manhã'],
                    'pu_base_abertura': df_contrato_vencimento.loc[index, 'PU Base Manhã'],
                }
        else:
            raise Exception('Data de referência para marcação a mercado não encontrada para o '
                            + 'vencimento')


# print(ConsultaDadosTesouroDireto().pu_taxa_abertura_contratos(
#     'NTN-B', '27/11/2020', '15/08/2024'))
# output
# {'dia': '27/11/2020', 'taxa_compra_abertura': 0.0196, 'taxa_venda_abertura': 0.0208,
#     'pu_compra_abertura': 3917.08, 'pu_venda_abertura': 3901.7, 'pu_base_abertura': 3898.38}
# print(ConsultaDadosTesouroDireto().pu_taxa_abertura_contratos(
#     'LTN', DatesBR().sub_working_days(DatesBR().curr_date(), 1), '01/01/2021'))
# output
# {'dia': '27/11/2020', 'taxa_compra_abertura': 0.0196, 'taxa_venda_abertura': 0.0208,
#     'pu_compra_abertura': 998.22, 'pu_venda_abertura': 998.12, 'pu_base_abertura': 998.04}
# print(ConsultaDadosTesouroDireto().pu_taxa_abertura_contratos(
#     'LTN', DatesBR().sub_working_days(DatesBR().curr_date(), 1), '01/01/2021'))
# # output
# {'dia': '27/11/2020', 'taxa_compra_abertura': 0.0196, 'taxa_venda_abertura': 0.0208,
#     'pu_compra_abertura': 998.22, 'pu_venda_abertura': 998.12, 'pu_base_abertura': 998.04}
# print(ConsultaDadosTesouroDireto().pu_taxa_abertura_contratos(
#     'NTN-C', DatesBR().sub_working_days(DatesBR().curr_date(), 1), '01/04/2021'))
# # output
# {'dia': '27/11/2020', 'taxa_compra_abertura': -0.032799999999999996, 'taxa_venda_abertura': -
#     0.0316, 'pu_compra_abertura': 5240.43, 'pu_venda_abertura': 5238.26, 'pu_base_abertura': 5222.04}
# print(ConsultaDadosTesouroDireto().pu_taxa_abertura_contratos(
#     'NTN-B', DatesBR().sub_working_days(DatesBR().curr_date(), 1), '15/08/2040'))
# # output
# {'dia': '27/11/2020', 'taxa_compra_abertura': 0.0404, 'taxa_venda_abertura': 0.0416,
#     'pu_compra_abertura': 4321.25, 'pu_venda_abertura': 4259.12, 'pu_base_abertura': 4255.17}
# print(ConsultaDadosTesouroDireto().pu_taxa_abertura_contratos(
#     'NTN-B', DatesBR().sub_working_days(DatesBR().curr_date(), 1), '15/08/2050'))
# # output
# {'dia': '27/11/2020', 'taxa_compra_abertura': 0.042300000000000004, 'taxa_venda_abertura': 0.0435,
#     'pu_compra_abertura': 4435.09, 'pu_venda_abertura': 4355.19, 'pu_base_abertura': 4351.11}
# print(ConsultaDadosTesouroDireto().pu_taxa_abertura_contratos(
#     'NTN-B', DatesBR().sub_working_days(DatesBR().curr_date(), 500), '15/08/2050'))
# {'dia': '03/12/2018', 'taxa_compra_abertura': 0.05, 'taxa_venda_abertura': 0.0512, 'pu_compra_abertura': 3713.98, 'pu_venda_abertura': 3649.07, 'pu_base_abertura': 3648.44}
# print(ConsultaDadosTesouroDireto().pu_taxa_abertura_contratos(
#     'NTN-B', '17/02/2021', '15/08/2024'))
# # output
# {'dia': '17/02/2021', 'taxa_compra_abertura': 0.0191, 'taxa_venda_abertura': 0.0203, 'pu_compra_abertura': 3923.48, 'pu_venda_abertura': 3908.67, 'pu_base_abertura': 3907.44}
