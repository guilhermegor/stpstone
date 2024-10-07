### LIBRARY TO COLLECT MACROECONOMIC INFORMATIONS FROM BRAZIL ###


import datetime
import yfinance as yf
import pandas as pd
from requests import request
from stpstone.settings.global_slots import YAML_BR_MACRO
from stpstone.handling_data.object import HandlingObjects
from stpstone.cals.handling_dates import DatesBR
from stpstone.handling_data.numbers import NumHandler
from stpstone.handling_data.json import JsonFiles
from stpstone.handling_data.html_parser import HtmlHndler
from stpstone.handling_data.str import StrHandler
from stpstone.loggs.db_logs import DBLogs


class Sidra:

    @property
    def rl_dts_indicators(self, str_pool_name=None, list_dicts=list()):
        '''
        REFERENCES: https://www.ibge.gov.br/calendario-indicadores-novoportal.html
        DOCSTRING: RELEASE DATES FROM MACRO SHORT-TERM INDICATORS
        INPUTS: -
        OUTPUTS: DATAFRAME
        '''
        # request html
        bs_html = HtmlHndler().html_bs_parser(
            YAML_BR_MACRO['sidra']['st_indicators']['url'], 
            bl_verify=YAML_BR_MACRO['sidra']['st_indicators']['bl_verify']
        )
        # looping within short-term indicators raw
        for el in [el.get_text() for el in bs_html.find_all('p')]:
            if StrHandler().match_string_like(el, '*' + YAML_BR_MACRO['sidra'][
                'st_indicators']['pool_names_like'] + '*') == True:
                str_pool_name = StrHandler().get_string_until_substr(
                    el, YAML_BR_MACRO['sidra']['st_indicators']['pool_names_like'])
            elif (str_pool_name != None) and len(el) > 1 and (
                StrHandler().match_string_like(
                    el, '*' + YAML_BR_MACRO['sidra']['st_indicators']['pool_ref_dt'] + '*') == True):
                list_dicts.append({
                    YAML_BR_MACRO['sidra']['st_indicators']['col_pool_name']: str_pool_name,
                    YAML_BR_MACRO['sidra']['st_indicators']['col_release_dt']: \
                        DatesBR().str_dates_to_datetime(
                            StrHandler().get_string_until_substr(el, 'Refer'), 
                            YAML_BR_MACRO['sidra']['st_indicators']['format_input_dt']
                        ),
                    YAML_BR_MACRO['sidra']['st_indicators']['col_month_calc']: \
                        StrHandler().get_string_after_substr(el, ': ')
                })
        # creating a dataframe from the serialized list
        df_rl_dts_macro = pd.DataFrame(list_dicts)
        # id duplicates
        ds_id_dup = df_rl_dts_macro.duplicated(subset=[
            YAML_BR_MACRO['sidra']['st_indicators']['col_pool_name']], keep='last')
        # invert the duplicate strategy and keep all repeated data, but the last
        df_rl_dts_macro = df_rl_dts_macro[ds_id_dup]
        # adding logging
        df_rl_dts_macro = DBLogs().audit_log(
            df_rl_dts_macro, YAML_BR_MACRO['sidra']['st_indicators']['url'], 
            DatesBR().curr_date
        )
        # return dataframe
        return df_rl_dts_macro

    def sidra_variables(self, aggregate, variable='all', location='BR', method='GET',
                        url='https://servicodados.ibge.gov.br/api/v3/agregados/{}/variaveis/{}'):
        '''
        REFERENCES: https://servicodados.ibge.gov.br/api/docs/agregados?versao=3
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        '''
        # payload
        dict_payload = {
            'localidades': location
        }
        # converting aggregate and variable to string
        aggregate, variable = str(aggregate), str(variable)
        # requesting data
        resp_req = request(method, url.format(aggregate, variable), data=dict_payload)
        # raises exception when not a 2xx response
        resp_req.raise_for_status()
        # retrieve json
        return resp_req.json()

    def sidra_modification_dates(self, aggregate, method='GET',
                                 url='https://servicodados.ibge.gov.br/api/v3/agregados/{}/periodos'):
        '''
        REFERENCES: https://servicodados.ibge.gov.br/api/docs/agregados?versao=3
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        '''
        # converting aggregate and variable to string
        aggregate = str(aggregate)
        # requesting data
        resp_req = request(method, url.format(aggregate))
        # raises exception when not a 2xx response
        resp_req.raise_for_status()
        # retrieve json
        return resp_req.json()

    @property
    def ipca_modification_dates(self, aggregate='1737'):
        '''
        REFERENCES: https://sidra.ibge.gov.br/tabela/1737
        DOCSTRING: INDICE NUMBER OF IPCA (INFLATION INDEX FOR BRAZILLIAN PURPOSES)
        INPUTS: -
        OUTPUTS: DICTIONARY (CLOSING MONTH AND INDICE NUMBER KEYS)
        '''
        return self.sidra_modification_dates(aggregate)

    @property
    def ipca_15_modification_dates(self, aggregate='3065'):
        '''
        REFERENCES: https://sidra.ibge.gov.br/tabela/3065
        DOCSTRING: INDICE NUMBER OF IPCA 15 (INFLATION INDEX FOR BRAZILLIAN PURPOSES)
        INPUTS: -
        OUTPUTS: DICTIONARY (CLOSING MONTH AND INDICE NUMBER KEYS)
        '''
        return self.sidra_modification_dates(aggregate)

    @property
    def ipca_in_index_number(self, aggregate='1737', variable='2266'):
        '''
        REFERENCES: https://sidra.ibge.gov.br/tabela/1737
        DOCSTRING: INDICE NUMBER OF IPCA (INFLATION INDEX FOR BRAZILLIAN PURPOSES)
        INPUTS: -
        OUTPUTS: DICTIONARY (CLOSING MONTH AND INDICE NUMBER KEYS)
        '''
        return self.sidra_variables(aggregate, variable)[0]['resultados'][0]['series'][0]['serie']

    @property
    def ipca_15_in_index_number(self, aggregate='3065', variable='1117'):
        '''
        REFERENCES: https://sidra.ibge.gov.br/tabela/3065
        DOCSTRING: INDICE NUMBER OF IPCA 15 (INFLATION INDEX FOR BRAZILLIAN PURPOSES)
        INPUTS: -
        OUTPUTS: DICTIONARY (CLOSING MONTH AND INDICE NUMBER KEYS)
        '''
        return self.sidra_variables(aggregate, variable)[0]['resultados'][0]['series'][0]['serie']


class BCB:

    def sgs_bcb(self, int_sgs_tabel, data_inic, data_fim):
        '''
        REFERENCES: https://dadosabertos.bcb.gov.br/dataset/20542-saldo-da-carteira-de-credito-com-recursos-livres---total/resource/6e2b0c97-afab-4790-b8aa-b9542923cf88
        DOCSTRING: INFOS FROM SGS BCB REST
        INPUTS: INPUTS, INCIAL AND END DATES ('DD/MM/YYYY' OR DATETIME FORMAT)
        OUPUTS: SGS RESPONSE
        '''
        # adjusting variables types
        if isinstance(data_inic, datetime.date) == True:
            data_inic = DatesBR().datetime_to_string(data_inic)
        if isinstance(data_fim, datetime.date) == True:
            data_fim = DatesBR().datetime_to_string(data_fim)
        # url and dict_payload to communicate to rest
        url = 'http://api.bcb.gov.br/dados/serie/bcdata.sgs.{}/dados'.format(int_sgs_tabel)
        query_params = {
            'formato': 'json',
            'dataInicial': data_inic,
            'dataFinal': data_fim
        }
        # retorno do dado de interesse
        message_content = request('GET', url, params=query_params).content
        return HandlingObjects().literal_eval_data(message_content, "b'", "'")

    def igpm(self, data_inic, data_fim, tabela_sgs_bcb='189'):
        '''
        REFERENCES: https://www3.bcb.gov.br/sgspub/consultarvalores/telaCvsSelecionarSeries.paint
        DOCSTRING: IGPM TIMES SERIES
        INPUTS: INICIAL DATE, FINAL DATE, AND SGS BCB TABLE
        OUTPUTS: LIST OF DICTS (DATE, VALUE)
        '''
        return self.sgs_bcb(tabela_sgs_bcb, data_inic, data_fim)

    def selic_daily(self, data_inic, data_fim, tabela_sgs_bcb='11'):
        '''
        REFERENCES: https://www3.bcb.gov.br/sgspub/consultarvalores/telaCvsSelecionarSeries.paint
        DOCSTRING: DAILY SELIC
        INPUTS: INICIAL DATE, FINAL DATE, AND SGS BCB TABLE
        OUTPUTS: LIST OF DICTS (DATE, VALUE)
        '''
        return self.sgs_bcb(tabela_sgs_bcb, data_inic, data_fim)

    def selic_target(self, data_inic, data_fim, tabela_sgs_bcb='432'):
        '''
        REFERENCES: https://www3.bcb.gov.br/sgspub/consultarvalores/telaCvsSelecionarSeries.paint
        DOCSTRING: DAILY SELIC
        INPUTS: INICIAL DATE, FINAL DATE, AND SGS BCB TABLE
        OUTPUTS: LIST OF DICTS (DATE, VALUE)
        '''
        return self.sgs_bcb(tabela_sgs_bcb, data_inic, data_fim)

    def usd_brl(self, data_ref, formato_data_saida='%m/%d/%Y', int_dol_usd_security_id=9800508,
                key_dol_usd_security_id='securityIdentificationCode', key_value='value',
                key_last_update='lastUpdate', int_precision_decimals=4,
                endpoint_taxas_referenciais_b3='https://sistemaswebb3-derivativos.b3.com.br/financialIndicatorsProxy/ReferenceExchangeRate/GetReferenceExchangeRate/eyJsYW5ndWFnZSI6InB0LWJyIn0=',
                method_req='GET', bl_verify=False):
        '''
        REFERENCES: https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/aplicacao#!/recursos/CotacaoDolarDia#eyJmb3JtdWxhcmlvIjp7IiRmb3JtYXQiOiJqc29uIiwiJHRvcCI6MTAwLCJkYXRhQ290YWNhbyI6IjEyLTA5LTIwMjAifSwicHJvcHJpZWRhZGVzIjpbMCwxLDJdfQ==
        DOCSTRING: COMMERCIAL DOL/BRL IN DAILY OBSERVATIONS
        INPUTS: DATE (WHETHER DATETIME OR 'DD/MM/YYYY')
        OUTPUTS: JSON
        '''
        # dateformat adjustment
        if DatesBR().check_date_datetime_format(data_ref) == True:
            data_ref = DatesBR().datetime_to_string(data_ref, formato_data_saida)
        else:
            data_ref = DatesBR().datetime_to_string(DatesBR().str_dates_to_datetime(data_ref),
                                                    formato_data_saida)
        # verifying the consistence of data returned from BCB API; in 11/05/2021 there was a
        #   maintenance causing a temporarily unavailability, due to this problem was created a
        #   contingency to return the DOLBRL exchange from B3
        try:
            # request usd/brl
            url = "https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/CotacaoDolarDia(" \
                + "dataCotacao=@dataCotacao)?@dataCotacao={}&$top=100&$format=json&$".format(
                    "'" + data_ref + "'") \
                + "select=cotacaoCompra,cotacaoVenda,dataHoraCotacao"
            dict_payload = {}
            headers = {
                'Cookie': 'JSESSIONID=0000GNzWCNvJ8ltUKMUmrwlH_Y6:1cn7jtfnj; dtCookie=9C0BF4FB'
                + 'DA3E96A68FC297DBEDAE82BF|cHRheHwx; BIGipServer~App~upstream_was_ssl-p='
                + '1020268972.47873.0000'
            }
            request_message = request(
                "GET", url, headers=headers, data=dict_payload).content
            request_message = HandlingObjects().literal_eval_data(request_message, "b'", "'")
            return {
                'cotacao_compra': request_message['value'][0]['cotacaoCompra'],
                'cotacao_venda': request_message['value'][0]['cotacaoVenda'],
                'timestamp_cotacao': request_message['value'][0]['dataHoraCotacao'],
            }
        except:
            # consult the html from b3 reference interest taxes
            resp_req = request(
                method_req, endpoint_taxas_referenciais_b3, verify=bl_verify)
            # raise exception in case return is different from 2xx
            resp_req.raise_for_status()
            # returning content
            json_ref_taxes_b3 = resp_req.content
            # turning type from byte to dictionary
            json_ref_taxes_b3 = HandlingObjects().literal_eval_data(
                json_ref_taxes_b3, "b'", "'")
            # looping through dictionaries and identifying the one that refers to 1-day usd-brl
            dict_usd_brl = [dict_ for dict_ in json_ref_taxes_b3 if int(dict_[
                key_dol_usd_security_id]) == int(int_dol_usd_security_id)][0]
            # return the contingency dolbrl exchange
            return {
                'cotacao_compra': NumHandler().convert_thousands_decimals_separator(
                    dict_usd_brl[key_value], precision_decimals=int_precision_decimals),
                'cotacao_venda': NumHandler().convert_thousands_decimals_separator(
                    dict_usd_brl[key_value], precision_decimals=int_precision_decimals),
                'timestamp_cotacao': dict_usd_brl[key_last_update],
            }

    def foreign_exchange_bcb(self, int_working_days_before_end=1, int_working_days_before_begin=None,
                             url='https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/{}',
                             api_currencies='Moedas',
                             api_currecy_exchange_day='CotacaoMoedaDia(moeda=@moeda,dataCotacao=@dataCotacao)?{}',
                             api_curency_exchange_period='CotacaoMoedaPeriodo(moeda=@moeda,dataInicial=@dataInicial,'
                             + 'dataFinalCotacao=@dataFinalCotacao)?{}', method='GET',
                             key_value='value', key_simbolo='simbolo', str_currency='{}BRL', 
                             dict_export=dict()):
        '''
        REFERENCES: https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/swagger-ui3#/
        DOCSTRING: BRAZILLIAN FOREIGN EXCHANGE FROM ALL CURRENCIES AVAILABE IN OLINDA API FROM
            DOMESTIC CENTRAL BANK
        INPUTS: URL
        OUTPUTS:
        '''
        # creating reference datetime, in specific string type, according to working days before
        #   (0 as default)
        datetime_end_ref = DatesBR().sub_working_days(DatesBR().curr_date,
                                                      int_working_days_before_end)
        str_datetime_end_ref = datetime_end_ref.strftime('%m-%d-%Y')
        if int_working_days_before_begin != None:
            datetime_begin_ref = DatesBR().sub_working_days(DatesBR().curr_date,
                                                            int_working_days_before_end)
            str_datetime_begin_ref = datetime_begin_ref.strftime('%m-%d-%Y')
        # header
        dict_header = {
            'accept': 'application/json;odata.metadata=minimal'
        }
        # payload for currencies available consulting
        dict_payload = {
            'format': 'json'
        }
        dict_payload = JsonFiles().dict_to_json(dict_payload)
        # retriving data
        resp_req = request(method, url.format(api_currencies), headers=dict_header,
                           data=dict_payload)
        # raises exception when not a 2xx response
        resp_req.raise_for_status()
        # retrieving json
        json_currencies = resp_req.json()
        # collecting all currencies available (against BRL)
        list_dict_currencies = json_currencies[key_value]
        list_currencies = [str(dict_[key_simbolo])
                           for dict_ in list_dict_currencies]
        # requesting current foreign available for all the currencies disposed before
        for currency in list_currencies:
            #   validating wheter the user aim for a bucket of currencies in a given day or period
            if int_working_days_before_begin == None:
                #   payload for currencies available consulting
                dict_payload = {
                    'format': 'json',
                }
                dict_params = {
                    'moeda': "'{}'".format(str(currency)),
                    'dataCotacao': "'{}'".format(str(str_datetime_end_ref)),
                }
                #   coverting to string payload and params
                str_payload = '%40' + '&%40'.join(
                    '{}={}'.format(k, v) for k, v in 
                    dict_params.items()) + '&%24' + '%24'.join(
                        '{}={}'.format(k, v) for k, v in dict_payload.items())
                #   requesting currency exchange against brl
                resp_req = request(method, url.format(api_currecy_exchange_day.format(
                    str_payload)),
                    headers=dict_header)
            else:
                #   payload for currencies available consulting
                dict_payload = {
                    'format': 'json',
                }
                dict_params = {
                    'moeda': "'{}'".format(str(currency)),
                    'dataInicial': "'{}'".format(str(str_datetime_begin_ref)),
                    'dataFinalCotacao': "'{}'".format(str(str_datetime_end_ref)),
                }
                #   coverting to string payload and params
                str_payload = '%40' + '&%40'.join(
                    '{}={}'.format(k, v) for k, v in
                    dict_params.items()) + '&%24' + '%24'.join(
                        '{}={}'.format(k, v) for k, v in dict_payload.items())
                #   requesting currency exchange against brl
                resp_req = request(method, url.format(api_curency_exchange_period.format(
                    str_payload)), headers=dict_header)
            #   raises exception when not a 2xx response
            resp_req.raise_for_status()
            #   retrieving json
            json_currency_exchange = resp_req.json()
            #   appending to export dictionary
            dict_export[str_currency.format(
                currency)] = json_currency_exchange[key_value]
        # retrieving foreign exchanges
        return dict_export


class YFinanceMacroBR:

    @property
    def ipca_forecast(self, method='GET', url='https://sbcharts.investing.com/events_charts/eu/1165.json',
                      int_convert_miliseconds_seconds=1000):
        '''
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        '''
        # requesting data
        req_resp = request(method, url)
        # raises exception when not a 2xx response
        req_resp.raise_for_status()
        # retrieving json
        json_brazillian_cpi = req_resp.json()
        # getting historical data
        json_brazillian_cpi = [{
            'datetime': DatesBR().timestamp_float_to_datetime(
                int(int(dict_['timestamp']) / int_convert_miliseconds_seconds), bl_format=True),
            'actual_state': str(dict_['actual_state']),
            'actual': float(dict_['actual']),
            'forecast': dict_['forecast'],
            'revised': dict_['revised'],
        } for dict_ in json_brazillian_cpi['attr']]
        # retrieving historical data
        return json_brazillian_cpi
    
    def yforex(self, list_xcg_curr, wd_start_date=2, wd_end_date=0,
                               list_dicts=list()):
        '''
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        '''
        # creating dates of interest according to working days provided
        inf_date = DatesBR().sub_working_days(DatesBR().curr_date, wd_start_date).strftime(
            '%Y-%m-%d')
        sup_date = DatesBR().sub_working_days(DatesBR().curr_date, wd_end_date).strftime(
            '%Y-%m-%d')
        # looping within exchange currencies
        for xcg_curr in list_xcg_curr:
            #   dealing with raw exchange rate currency
            ticker = xcg_curr + "=X"
            #   importing historical data to dataframe
            df_ = yf.download(ticker, start=inf_date, end=sup_date, progress=False)
            #   adding currency column
            df_[YAML_BR_MACRO['exchange_rates_yahoo']['col_currency']] = xcg_curr
            #   adding reference date
            df_[YAML_BR_MACRO['exchange_rates_yahoo']['col_date'].upper()] = \
                df_.index.to_frame(name=YAML_BR_MACRO['exchange_rates_yahoo']['col_date'])
            #   appending to serialized list
            list_dicts.extend(df_.to_dict(orient='records'))
        # serialized list to dataframe
        df_xcg = pd.DataFrame(list_dicts)
        # renaming columns
        df_xcg = df_xcg.rename(columns={
            x: x.upper() for x in df_xcg.columns
        })
        # sort dataframe
        df_xcg.sort_values(
            [
                YAML_BR_MACRO['exchange_rates_yahoo']['col_currency'], 
                YAML_BR_MACRO['exchange_rates_yahoo']['col_date'].upper()
            ],
            ascending=[True, False],
            inplace=True
        )
        # adding logging to the last dataframe
        df_xcg = DBLogs().audit_log(
            df_xcg, 
            r'https://finance.yahoo.com/quote/',
            DatesBR().utc_from_dt(DatesBR().curr_date)
        )
        # return dataframe
        return df_xcg

# dict_serie_ipca = self.ipca_in_index_number
# float_ipca = dict_serie_ipca[list(dict_serie_ipca.keys())[-1]]
# print(float_ipca)
# # output
# {'closing_month': 'outubro 2020', 'indice_number': '5438.1200000000000'}

# print(self.ipca_forecast)

# print(self.ipca_15_in_index_number)
# output
# {'closing_month': 'outubro 2020', 'indice_number': '5438.1200000000000'}

# print(self.ipca_15_modification_dates)

# print(self.igpm('28/10/2020', '30/11/2020'))
# # outputs
# [{'data': '01/10/2020', 'valor': '3.23'}, {'data': '01/11/2020', 'valor': '3.28'}]

# print(self.selic_daily('01/11/2020', '30/11/2020'))
# # outputs
# [{'data': '30/10/2020', 'valor': '0.007469'}, {'data': '03/11/2020', 'valor': '0.007469'}, {'data': '04/11/2020', 'valor': '0.007469'}, {'data': '05/11/2020', 'valor': '0.007469'}, {'data': '06/11/2020', 'valor': '0.007469'}, {'data': '09/11/2020', 'valor': '0.007469'}, {'data': '10/11/2020', 'valor': '0.007469'}, {'data': '11/11/2020', 'valor': '0.007469'}, {'data': '12/11/2020', 'valor': '0.007469'}, {'data': '13/11/2020', 'valor': '0.007469'}, {'data': '16/11/2020',
#                                                                                                                                                                                                                                                                                                                                                                                                                                                                     'valor': '0.007469'}, {'data': '17/11/2020', 'valor': '0.007469'}, {'data': '18/11/2020', 'valor': '0.007469'}, {'data': '19/11/2020', 'valor': '0.007469'}, {'data': '20/11/2020', 'valor': '0.007469'}, {'data': '23/11/2020', 'valor': '0.007469'}, {'data': '24/11/2020', 'valor': '0.007469'}, {'data': '25/11/2020', 'valor': '0.007469'}, {'data': '26/11/2020', 'valor': '0.007469'}, {'data': '27/11/2020', 'valor': '0.007469'}, {'data': '30/11/2020', 'valor': '0.007469'}]

# print(self.selic_target('01/12/2022', '29/12/2022'))
# # output
# [{'data': '01/11/2020', 'valor': '2.00'}, {'data': '02/11/2020', 'valor': '2.00'}, {'data': '03/11/2020', 'valor': '2.00'}, {'data': '04/11/2020', 'valor': '2.00'}, {'data': '05/11/2020', 'valor': '2.00'}, {'data': '06/11/2020', 'valor': '2.00'}, {'data': '07/11/2020', 'valor': '2.00'}, {'data': '08/11/2020', 'valor': '2.00'}, {'data': '09/11/2020', 'valor': '2.00'}, {'data': '10/11/2020', 'valor': '2.00'}, {'data': '11/11/2020', 'valor': '2.00'}, {'data': '12/11/2020', 'valor': '2.00'}, {'data': '13/11/2020', 'valor': '2.00'}, {'data': '14/11/2020', 'valor': '2.00'}, {'data': '15/11/2020', 'valor': '2.00'}, {'data': '16/11/2020', 'valor': '2.00'}, {'data': '17/11/2020', 'valor': '2.00'}, {'data': '18/11/2020', 'valor': '2.00'}, {'data': '19/11/2020', 'valor': '2.00'}, {'data': '20/11/2020', 'valor': '2.00'}, {'data': '21/11/2020', 'valor': '2.00'}, {'data': '22/11/2020', 'valor': '2.00'}, {'data': '23/11/2020', 'valor': '2.00'}, {'data': '24/11/2020', 'valor': '2.00'}, {'data': '25/11/2020', 'valor': '2.00'}, {'data': '26/11/2020', 'valor': '2.00'}, {'data': '27/11/2020', 'valor': '2.00'}, {'data': '28/11/2020', 'valor': '2.00'}, {'data': '29/11/2020', 'valor': '2.00'}, {'data': '30/11/2020', 'valor': '2.00'}]

# print(self.ipca_in_index_number())
# # output
# {'closing_month': 'outubro 2020', 'indice_number': '5438.1200000000000'}

# print(self.usd_brl('01/12/2017'))
# # output
# {'cotacao_compra': 3.263, 'cotacao_venda': 3.2636, 'timestamp_cotacao': '2017-12-01 13:05:49.223'}

# print(self.selic_daily('10/12/2020', '10/12/2020'))

# # output
# [{'data': '10/12/2020', 'valor': '0.007469'}]

# print(self.selic_daily('13/02/2021', '13/02/2021'))
# # output
# [{'data': '12/02/2021', 'valor': '0.007469'}]

# print(self.usd_brl('05/11/2021'))
# # output
# #   {'cotacao_compra': 5.6163, 'cotacao_venda': 5.6163, 'timestamp_cotacao': '20211104'}

# pprint(self.yforex(['USDBRL', 'CADEUR']))

# pprint(self.foreign_exchange_bcb())