### LIBRARY TO COLLECT MACROECONOMIC INFORMATIONS FROM BRAZIL ###


import datetime
import backoff
import yfinance as yf
import pandas as pd
from requests import request
from stpstone._config.global_slots import YAML_BR_MACRO
from stpstone.handling_data.object import HandlingObjects
from stpstone.utils.cals.handling_dates import DatesBR
from stpstone.handling_data.numbers import NumHandler
from stpstone.utils.parsers.json import JsonFiles
from stpstone.utils.parsers.html import HtmlHandler
from stpstone.utils.parsers.str import StrHandler
from stpstone.utils.loggs.db_logs import DBLogs


class BCB:

    @property
    def market_macro_expec(self, url='https://olinda.bcb.gov.br/olinda/servico/Expectativas/versao/v1/odata/ExpectativasMercadoAnuais?$top=100000&$orderby=Data%20desc&$format=json&$select=Indicador,IndicadorDetalhe,Data,DataReferencia,Media,Mediana,Minimo,Maximo,numeroRespondentes',
        col_indicator='Indicador', col_detailed_indicator='IndicadorDetalhe', col_date='Data', col_ref_year='DataReferencia',
        col_avg='Media', col_median='Mediana', col_min='Minimo', col_max='Maximo', col_num_answ='numeroRespondentes'):
        """
        DOCSTRING: ANNUAL MARKET EXPECTATIONS FROM THE BRAZILIAN CENTRAL BANK, INCLUDING INDICATORS LIKE GDP
            GROWTH AND INFLATION, WITH STATISTICS ON AVERAGE, MEDIAN, MIN, MAX VALUES, AND RESPONDENT COUNT.
        INPUTS: -
        OUTPUTS: DATAFRAME
        """
        # request olinda bcb
        dict_headers = {
            'accept': 'application/json',
            'client_id': 'E0Bbo4L19nlx',
            'access_token': 's56HuH4yFasr',
            'Cookie': 'BIGipServer~was_p_as3~was_p~pool_was_443_p=4275048876.47873.0000; JSESSIONID=0000X4IrBKiAUyQvbYXXFfX0gne:1dof89mke; TS013694c2=012e4f88b3c6fee6e3a792e5d4f68cb31972d27ba778ec1e05a622b5b87ecf0bda522fe8652f85210b7cbe2b227fe76a647ca3acc6'
        }
        req_resp = request('GET', url, headers=dict_headers)
        req_resp.raise_for_status()
        json_bcb_expec = req_resp.json()
        # load to pandas dataframe
        df_expec_bcb = pd.DataFrame(json_bcb_expec['value'])
        # changing columns types
        df_expec_bcb = df_expec_bcb.astype({
            col_indicator: str,
            col_detailed_indicator: str,
            col_date: str,
            col_ref_year: int,
            col_avg: float,
            col_median: float,
            col_min: float,
            col_max: float,
            col_num_answ: int
        })
        # return dataframe
        return df_expec_bcb


class YFinanceMacroBR:

    @property
    def ipca_forecast(self, method='GET', url='https://sbcharts.investing.com/events_charts/eu/1165.json',
                      int_convert_miliseconds_seconds=1000):
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        # requesting data
        req_resp = request(method, url)
        # raises exception when not a 2xx response
        req_resp.raise_for_status()
        # retrieving json
        json_brazillian_cpi = req_resp.json()
        # getting historical data
        json_brazillian_cpi = [{
            'datetime': DatesBR().unix_timestamp_to_datetime(
                int(int(dict_['timestamp']) / int_convert_miliseconds_seconds), bl_format=True),
            'actual_state': str(dict_['actual_state']),
            'actual': float(dict_['actual']),
            'forecast': dict_['forecast'],
            'revised': dict_['revised'],
        } for dict_ in json_brazillian_cpi['attr']]
        # retrieving historical data
        return json_brazillian_cpi

    def ycurrency(self, list_xcg_curr, wd_start_date=2, wd_sup_date=0):
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        # setting variables
        list_ser = list()
        # creating dates of interest according to working days provided
        inf_date = DatesBR().sub_working_days(DatesBR().curr_date, wd_start_date).strftime(
            '%Y-%m-%d')
        sup_date = DatesBR().sub_working_days(DatesBR().curr_date, wd_sup_date).strftime(
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
            list_ser.extend(df_.to_dict(orient='records'))
        # serialized list to dataframe
        df_xcg = pd.DataFrame(list_ser)
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
