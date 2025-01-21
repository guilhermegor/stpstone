### COLLECTING UP2DATA B3 INFORMATION - ROOT URL: https://arquivos.b3.com.br/Web/Consolidated ###

# pypi.org libs
import pandas as pd
from requests import request
from zipfile import ZipFile
from datetime import datetime
from typing import Union
# private modules
from stpstone.settings._global_slots import YAML_B3
from stpstone.opening_config.setup import iniciating_logging
from stpstone.loggs.create_logs import CreateLog
from stpstone.cals.handling_dates import DatesBR
from stpstone.handling_data.json import JsonFiles
from stpstone.handling_data.object import HandlingObjects
from stpstone.handling_data.lists import HandlingLists
from stpstone.directories_files_manag.managing_ff import DirFilesManagement
from stpstone.cals.handling_dates import DatesBR
from stpstone.handling_data.lists import StrHandler
from stpstone.handling_data.xml import XMLFiles
from stpstone.loggs.db_logs import DBLogs
from stpstone.pool_conn.session import ReqSession
from stpstone.handling_data.pd import DealingPd
from stpstone.meta.validate_pm import ValidateAllMethodsMeta


class UP2DATAB3(metaclass=ValidateAllMethodsMeta):

    def __init__(self, dt_ref:datetime=DatesBR().sub_working_days(DatesBR().curr_date, 1), 
                 req_session:Union[ReqSession, None]=None, bl_use_timer:bool=False) -> None:
        '''
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        '''
        self.dt_ref = dt_ref
        self.req_session = ReqSession(bl_use_timer=bl_use_timer) \
            if req_session is None else req_session
        self.token = self.access_token
    
    @property
    def access_token(self) -> str:
        '''
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        '''
        url_token = YAML_B3['up2data_b3']['request_token'].format(self.dt_ref)
        req_resp = self.req_session.session.get(url_token)
        req_resp.raise_for_status()
        return req_resp.json()[YAML_B3['up2data_b3']['key_token']]
    
    @property
    def instruments_register(self) -> pd.DataFrame:
        '''
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        '''
        # setting variables
        list_ser = list()
        # requesting data
        resp_req = self.req_session.session.get(YAML_B3['up2data_b3']['access_api'].format(self.token))
        resp_req.raise_for_status()
        str_resp_req = resp_req.text
        # cleaning response
        list_resp_req = str_resp_req.split('\n')
        list_headers = list_resp_req[1].split(';')
        list_data = list_resp_req[2:]
        for row in list_data:
            if len(row) == 0: continue
            list_row = row.split(';')
            list_ser.append(dict(zip(list_headers, list_row)))
        # dataframe from serialized list
        df_ = pd.DataFrame(list_ser)
        # changing column types
        list_cols_df = list(df_.columns)
        df_ = df_.astype(dict(zip(list_cols_df, [str] * len(list_cols_df))))
        # adding logging
        df_ = DBLogs().audit_log(
            df_, 
            DatesBR().utc_from_dt(DatesBR().curr_date), 
            DatesBR().utc_log_ts
        )
        # returning instruments register
        return df_
    
    @property
    def daily_trades_secondary_market(self) -> pd.DataFrame:
        '''
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        '''
        # requesting daily trades
        file_daily_trades = DirFilesManagement().get_zip_from_web_in_memory(
            YAML_B3['up2data_b3']['url_daily_trades'].format(self.dt_ref.strftime('%Y-%m-%d')), 
            bl_io_interpreting=YAML_B3['up2data_b3']['bl_io_interpreting'],
            bl_verify=YAML_B3['up2data_b3']['bl_verify']
        )
        # importing to pandas dataframe
        reader = pd.read_csv(
            file_daily_trades,
            sep=YAML_B3['up2data_b3']['str_sep'], 
            decimal=YAML_B3['up2data_b3']['str_decimal']
        )
        df_daily_trades = pd.DataFrame(reader)
        # substituindo valores nulos
        df_daily_trades.fillna(YAML_B3['up2data_b3']['value_nan'], inplace=True)
        # changing columns types
        df_daily_trades = DealingPd().change_dtypes(
            df_daily_trades, 
            {
                YAML_B3['up2data_b3']['col_dt_ref']: str,
                YAML_B3['up2data_b3']['col_instrument']: str,
                YAML_B3['up2data_b3']['col_stock_update']: int,
                YAML_B3['up2data_b3']['col_pv']: float,
                YAML_B3['up2data_b3']['col_qty_trd']: int,
                YAML_B3['up2data_b3']['col_closing_time']: int,
                YAML_B3['up2data_b3']['col_trd_code']: int,
                YAML_B3['up2data_b3']['col_section_type']: int,
                YAML_B3['up2data_b3']['col_trd_dt']: str,
                YAML_B3['up2data_b3']['col_buyer_code']: int,
                YAML_B3['up2data_b3']['col_seller_code']: int
            }, 
            [YAML_B3['up2data_b3']['col_dt_ref'], YAML_B3['up2data_b3']['col_trd_dt']], 
            errors='ignore'
        )
        # column asset
        df_daily_trades[YAML_B3['up2data_b3']['col_create_asset']] = [
            row[YAML_B3['up2data_b3']['col_instrument']][:3] 
            for _, row in df_daily_trades.iterrows()
        ]
        # adding logging
        df_daily_trades = DBLogs().audit_log(
            df_daily_trades, 
            DatesBR().utc_from_dt(self.dt_ref), 
            DatesBR().utc_log_ts
        )
        # returning dataframe
        return df_daily_trades

    @property
    def security_category_name(self, dict_export=dict(), key_ticker='TckrSymb',
                               col_security_category_name='SctyCtgyNm',
                               col_market_name='MktNm',
                               list_markets_classified=list(),
                               bl_return_markets_not_classified=False):
        '''
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        '''
        # fetch in memory instruments register of assets traded in b3 exchange
        df_ = self.instruments_register_raw
        print('*** REIGSTER B3 RAW ***')
        print(df_)
        # creating dictionary with instruments according to each type of market
        for security_division, col_ in [
            ('securities_by_category_name', col_security_category_name),
                ('securities_by_market_name', col_market_name)]:
            for market, list_source_names in YAML_B3['up2data_b3'][security_division].items():
                #   validating wheter the market is already in the exporting dictionary and extending
                #       or creating a new list
                if market not in dict_export:
                    dict_export[market] = HandlingLists().remove_duplicates(
                        df_[
                            df_[col_].isin(
                                list_source_names)][key_ticker].tolist())
                else:
                    dict_export[market].extend(HandlingLists().remove_duplicates(
                        df_[
                            df_[col_].isin(
                                list_source_names)][key_ticker].tolist()))
                list_markets_classified.extend(list_source_names)
        # defining markets not classified, if is user's will
        if bl_return_markets_not_classified == True:
            return HandlingLists().remove_duplicates(
                df_[
                    ~df_[col_security_category_name].isin(
                        list_markets_classified)][col_security_category_name].tolist())
        else:
            #   returing dictionary with tickers accordingly to the market type which participates
            return dict_export

    def loan_balace(self, wd_bef=1, url='https://arquivos.b3.com.br/tabelas/table/LoanBalance/{}/{}', 
                    method='GET', key_pg_count='pageCount', key_valores='values', 
                    list_cols_lb_b3=['RptDt', 'TckrSymb', 'Asst', 'QtyCtrctsDay', 'QtyShrDay', 
                                     'DnrMinRate', 'DnrAvrgRate', 'DnrMaxRate', 'TakrMinRate', 
                                     'TakrAvrgRate', 'TakrMaxRate', 'MktNm'], 
                    list_ser=list()):
        '''
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        '''
        # reference date
        data_ref_lb_b3 = DatesBR().sub_working_days_before(
            DatesBR().curr_date, wd_bef).strftime('%Y-%m-%d')
        # payload
        dict_payload = {}
        # headers
        dict_headers = {
            'authority': 'arquivos.b3.com.br',
            'accept': '*/*',
            'accept-language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
            'cookie': 'OptanonAlertBoxClosed=2023-03-24T13:33:20.482Z; _ga_T6D1M7G116=GS1.1.1684332031.17.0.1684332031.60.0.0; _tt_enable_cookie=1; _ttp=-3ewIEHyq8lWLAqJVQ6O6T5fGqv; _ga_X5KRPBP7ZE=GS1.3.1690829338.5.1.1690829690.60.0.0; _ga_0W7NXV5699=GS1.1.1695259533.1.0.1695259540.0.0.0; lumClientId=FF8080818A6B1B5C018ABCE5D291602D; lumMonUid=l_QNKA5sbq-l9hkDYeDpkuHl5Qvn8pSA; _ga_F2HQZ24Y48=GS1.1.1695386295.1.1.1695386413.36.0.0; visid_incap_2246223=zCpOWadTSVqdEX/oNXZmLb0vl2UAAAAAQUIPAAAAAAArXUd3AyxcF+G4fbInAZ/+; _gcl_au=1.1.1328610258.1704896955; _gcl_aw=GCL.1709150038.CjwKCAiA0PuuBhBsEiwAS7fsNTm-phWyGfd21MQsHApfiDnWpzvVnpH-JCKFzGOxLwGYnYogLKSlFBoCQXQQAvD_BwE; _ga_5E2DT9ZLVR=GS1.1.1709150038.3.1.1709150112.60.0.0; _ga_CNJN5WQC5G=GS1.1.1709239346.20.0.1709239476.0.0.0; _ga_CRYYSCYF12=GS1.1.1709825617.2.1.1709825617.60.0.0; _ga_FTT9L7SR7B=GS1.1.1709827350.1.1.1709829244.54.0.0; nlbi_2246223=LPzcM1dvj3Qj9PJY9OkOmwAAAAAHcT0kwIANYP4of6jRlfTo; incap_ses_1239_2246223=V2rbSa8xMQgKwd16LtExEbuC+WUAAAAA6Bx9qzgrW2qvO8fGb1s7Ug==; dtCookie=v_4_srv_33_sn_B96EE88FC773C73EDD967E7FE8D41023_perc_100000_ol_0_mul_1_app-3Afd69ce40c52bd20e_0_rcs-3Acss_0; _gid=GA1.3.971339064.1710850908; _clck=v6volp%7C2%7Cfk7%7C0%7C1232; auth0=; TS0171d45d=011d592ce15f7772eec4672b5224a65a3ed9ad68450822231fe43032d6783dbc2d826bb431969fefc876b99ec27a5a6eacbd316945; _ga_SS7FXRTPP3=GS1.1.1710850908.138.1.1710852407.59.0.0; _ga=GA1.3.154973072.1679664794; OptanonConsent=isGpcEnabled=0&datestamp=Tue+Mar+19+2024+09%3A46%3A47+GMT-0300+(Hor%C3%A1rio+Padr%C3%A3o+de+Bras%C3%ADlia)&version=6.21.0&isIABGlobal=false&hosts=&landingPath=NotLandingPage&groups=C0003%3A1%2CC0001%3A1%2CC0004%3A1%2CC0002%3A1&geolocation=%3B&AwaitingReconsent=false; _clsk=hrfzvd%7C1710852408467%7C8%7C1%7Cb.clarity.ms%2Fcollect; incap_ses_1239_2246223=9YpWOoNjqRP6zeF6LtExEfGI+WUAAAAASCaRRjwRJW3uB4oCgh0+dg==; nlbi_2246223=fL1wUKg2iFtTP+KU9OkOmwAAAABL1G2EaiPMxRnkbfBf01Ql; visid_incap_2246223=WX+mv0j8SJ2ikziYIwjLooCG+WUAAAAAQUIPAAAAAAD1iQw5sjPBQa6Xku7lrpeD; auth0=',
            'sec-ch-ua': '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
            'x-elastica_gw': '3.116.0'
        }
        # retrieving loan balance b3
        resp_req = request(method, url.format(data_ref_lb_b3, 1), headers=dict_headers, 
                           data=dict_payload)
        # raise error when status code is different from 2xx
        resp_req.raise_for_status()
        # storying current json in memory
        json_lb_b3 = resp_req.json()
        # verificando o número de páginas a serem consultadas
        int_pgs_lb_b3 = json_lb_b3[key_pg_count]
        # loopando em torno das páginas de interesse
        for i in range(1, int_pgs_lb_b3 + 1):
            #   retrieving loan balance b3
            resp_req = request(method, url.format(data_ref_lb_b3, i), headers=dict_headers, 
                               data=dict_payload)
            #   raise error when status code is different from 2xx
            resp_req.raise_for_status()
            #   storying current json in memory
            json_lb_b3 = resp_req.json()
            #   looping within available values
            for list_ in json_lb_b3[key_valores]:
                #   appending to serialized list
                list_ser.append(
                    dict(zip(list_cols_lb_b3, list_))
                )
        # builiding dataframe
        df_lb_b3 = pd.DataFrame(list_ser)
        # dropping duplicates
        df_lb_b3.drop_duplicates(inplace=True)
        # changing column types
        df_lb_b3 = df_lb_b3.astype({
            list_cols_lb_b3[0]: str,
            list_cols_lb_b3[1]: str,
            list_cols_lb_b3[2]: str,
            list_cols_lb_b3[3]: float,
            list_cols_lb_b3[4]: float,
            list_cols_lb_b3[5]: float,
            list_cols_lb_b3[6]: float,
            list_cols_lb_b3[7]: float,
            list_cols_lb_b3[8]: float,
            list_cols_lb_b3[9]: float,
            list_cols_lb_b3[10]: float,
            list_cols_lb_b3[11]: str
        })
        df_lb_b3[list_cols_lb_b3[0]] = [DatesBR().timestamp_separator_string_to_datetime(d) 
                                        for d in df_lb_b3[list_cols_lb_b3[0]]]
        # sorting dataframe
        df_lb_b3.sort_values([
            list_cols_lb_b3[0],
            list_cols_lb_b3[1],
            list_cols_lb_b3[2],
            list_cols_lb_b3[-1]
        ], ascending=[False, True, True, True], inplace=True)
        # adding logging
        df_lb_b3 = DBLogs().audit_log(
            df_lb_b3, 
            DatesBR().utc_from_dt(
                DatesBR().sub_working_days(DatesBR().curr_date, wd_bef)
            ), 
            DatesBR().utc_log_ts
        )
        # returning dataframe
        return df_lb_b3

    def lending_open_position(self, wd_bef=1, url='https://arquivos.b3.com.br/tabelas/table/LendingOpenPosition/{}/{}', 
                              method='GET', key_pg_count='pageCount', key_valores='values', 
                              list_cols_lop_b3=['RptDt', 'TckrSymb', 'Asst', 'BalQty', 'TradAvrgPric', 'PricFctr', 'BalVal'], 
                              list_ser=list()):
        '''
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        '''
        # reference date
        data_ref_lb_b3 = DatesBR().sub_working_days_before(
            DatesBR().curr_date, wd_bef).strftime('%Y-%m-%d')
        # payload
        dict_payload = {}
        # headers
        dict_headers = {
            'authority': 'arquivos.b3.com.br',
            'accept': '*/*',
            'accept-language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
            'cookie': 'OptanonAlertBoxClosed=2023-03-24T13:33:20.482Z; _ga_T6D1M7G116=GS1.1.1684332031.17.0.1684332031.60.0.0; _tt_enable_cookie=1; _ttp=-3ewIEHyq8lWLAqJVQ6O6T5fGqv; _ga_X5KRPBP7ZE=GS1.3.1690829338.5.1.1690829690.60.0.0; _ga_0W7NXV5699=GS1.1.1695259533.1.0.1695259540.0.0.0; lumClientId=FF8080818A6B1B5C018ABCE5D291602D; lumMonUid=l_QNKA5sbq-l9hkDYeDpkuHl5Qvn8pSA; _ga_F2HQZ24Y48=GS1.1.1695386295.1.1.1695386413.36.0.0; visid_incap_2246223=zCpOWadTSVqdEX/oNXZmLb0vl2UAAAAAQUIPAAAAAAArXUd3AyxcF+G4fbInAZ/+; _gcl_au=1.1.1328610258.1704896955; _gcl_aw=GCL.1709150038.CjwKCAiA0PuuBhBsEiwAS7fsNTm-phWyGfd21MQsHApfiDnWpzvVnpH-JCKFzGOxLwGYnYogLKSlFBoCQXQQAvD_BwE; _ga_5E2DT9ZLVR=GS1.1.1709150038.3.1.1709150112.60.0.0; _ga_CNJN5WQC5G=GS1.1.1709239346.20.0.1709239476.0.0.0; _ga_CRYYSCYF12=GS1.1.1709825617.2.1.1709825617.60.0.0; _ga_FTT9L7SR7B=GS1.1.1709827350.1.1.1709829244.54.0.0; nlbi_2246223=LPzcM1dvj3Qj9PJY9OkOmwAAAAAHcT0kwIANYP4of6jRlfTo; incap_ses_1239_2246223=V2rbSa8xMQgKwd16LtExEbuC+WUAAAAA6Bx9qzgrW2qvO8fGb1s7Ug==; dtCookie=v_4_srv_33_sn_B96EE88FC773C73EDD967E7FE8D41023_perc_100000_ol_0_mul_1_app-3Afd69ce40c52bd20e_0_rcs-3Acss_0; _gid=GA1.3.971339064.1710850908; _clck=v6volp%7C2%7Cfk7%7C0%7C1232; auth0=; TS0171d45d=011d592ce15f7772eec4672b5224a65a3ed9ad68450822231fe43032d6783dbc2d826bb431969fefc876b99ec27a5a6eacbd316945; _ga_SS7FXRTPP3=GS1.1.1710850908.138.1.1710852407.59.0.0; _ga=GA1.3.154973072.1679664794; OptanonConsent=isGpcEnabled=0&datestamp=Tue+Mar+19+2024+09%3A46%3A47+GMT-0300+(Hor%C3%A1rio+Padr%C3%A3o+de+Bras%C3%ADlia)&version=6.21.0&isIABGlobal=false&hosts=&landingPath=NotLandingPage&groups=C0003%3A1%2CC0001%3A1%2CC0004%3A1%2CC0002%3A1&geolocation=%3B&AwaitingReconsent=false; _clsk=hrfzvd%7C1710852408467%7C8%7C1%7Cb.clarity.ms%2Fcollect; incap_ses_1239_2246223=9YpWOoNjqRP6zeF6LtExEfGI+WUAAAAASCaRRjwRJW3uB4oCgh0+dg==; nlbi_2246223=fL1wUKg2iFtTP+KU9OkOmwAAAABL1G2EaiPMxRnkbfBf01Ql; visid_incap_2246223=WX+mv0j8SJ2ikziYIwjLooCG+WUAAAAAQUIPAAAAAAD1iQw5sjPBQa6Xku7lrpeD; auth0=',
            'sec-ch-ua': '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
            'x-elastica_gw': '3.116.0'
        }
        # retrieving loan balance b3
        resp_req = request(method, url.format(data_ref_lb_b3, 1), headers=dict_headers, 
                           data=dict_payload)
        # raise error when status code is different from 2xx
        resp_req.raise_for_status()
        # storying current json in memory
        json_lb_b3 = resp_req.json()
        # verificando o número de páginas a serem consultadas
        int_pgs_lb_b3 = json_lb_b3[key_pg_count]
        # loopando em torno das páginas de interesse
        for i in range(1, int_pgs_lb_b3 + 1):
            #   retrieving loan balance b3
            resp_req = request(method, url.format(data_ref_lb_b3, i), headers=dict_headers, 
                               data=dict_payload)
            #   raise error when status code is different from 2xx
            resp_req.raise_for_status()
            #   storying current json in memory
            json_lb_b3 = resp_req.json()
            #   looping within available values
            for list_ in json_lb_b3[key_valores]:
                #   appending to serialized list
                list_ser.append(
                    dict(zip(list_cols_lop_b3, list_))
                )
        # builiding dataframe
        df_lop_b3 = pd.DataFrame(list_ser)
        # dropping duplicates
        df_lop_b3.drop_duplicates(inplace=True)
        # changing column types
        df_lop_b3 = df_lop_b3.astype({
            list_cols_lop_b3[0]: str,
            list_cols_lop_b3[1]: str,
            list_cols_lop_b3[2]: str,
            list_cols_lop_b3[3]: float,
            list_cols_lop_b3[4]: float,
            list_cols_lop_b3[5]: float,
            list_cols_lop_b3[6]: float
        })
        df_lop_b3[list_cols_lop_b3[0]] = [DatesBR().timestamp_separator_string_to_datetime(d) 
                                        for d in df_lop_b3[list_cols_lop_b3[0]]]
        # sorting dataframe
        df_lop_b3.sort_values([
            list_cols_lop_b3[0],
            list_cols_lop_b3[1],
            list_cols_lop_b3[2],
            list_cols_lop_b3[-1]
        ], ascending=[False, True, True, True], inplace=True)
        # adding logging
        df_lop_b3 = DBLogs().audit_log(
            df_lop_b3, 
            DatesBR().utc_from_dt(
                DatesBR().sub_working_days(DatesBR().curr_date, wd_bef)
            ), 
            DatesBR().utc_log_ts
        )
        # returning dataframe
        return df_lop_b3
    
    @property
    def bond_issuers_accepted_warranty_b3(self, url_banks_brazil='http://www.bcb.gov.br/pom/spb/estatistica/port/ParticipantesSTRport.csv', 
        url_bond_issuers_warranty_b3='https://www.b3.com.br/data/files/86/34/44/30/407E971089F29C97AC094EA8/Emissores%20Aceitos%20-%20Titulos%20Privados.zip', 
        bl_io_interpreting=False, col_issuers='ISSUERS', col_ispb='ISPB', col_numero_cod='NUMERO_CODIGO', 
        col_nome_extenso='NOME_EXTENSO', col_fr_total='FRTotal',
        dict_replace_str_name= {
            'S/A': '',
            'S.A.': '', 
            'S.A': '',
            ' SA': '',
            'LTDA.': '',
            'LTDA': '',
            '(BRASIL)': '', 
            'Brasil S.A.': '',
            'BRASIL S A': '',
            'DE INVESTIMENTO': '', 
            'DE INVEST. ': '', 
            'BCO': 'BANCO', 
            'J.P.MORGAN': 'J.P. MORGAN', 
            'CHINA BRASIL': 'CHINA'
        }):
        '''
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        '''
        # importing to memory banks currently operating domestically
        reader = pd.read_csv(url_banks_brazil)
        df_banks_operating_domestically = pd.DataFrame(reader)
        # change column names
        df_banks_operating_domestically.rename(columns=dict(zip(
            df_banks_operating_domestically.columns, 
            [StrHandler().remove_diacritics(str(x).upper()) for x in 
                df_banks_operating_domestically.columns])), inplace=True)
        # defining list of columns
        list_cols = df_banks_operating_domestically.columns
        # change columns types
        df_banks_operating_domestically.fillna(0, inplace=True)
        df_banks_operating_domestically = df_banks_operating_domestically.astype({
            col_ispb: int,
            col_numero_cod: int
        })
        df_banks_operating_domestically = df_banks_operating_domestically.astype({
            col_ispb: str,
            col_numero_cod: str,
            col_nome_extenso: str, 
            col_fr_total: float
        })
        # dealing with complete names from financial institutions
        df_banks_operating_domestically[col_nome_extenso] = [StrHandler().remove_diacritics(
            StrHandler().replace_all(str(x).upper(), dict_replace_str_name).strip()) 
            for x in df_banks_operating_domestically[col_nome_extenso].tolist()]
        # importing to memory banks accepted as warranty in b3 exchange stock market
        list_unziped_files = DirFilesManagement().get_zip_from_web_in_memory(
            url_bond_issuers_warranty_b3, 
            bl_io_interpreting=bl_io_interpreting)
        if type(list_unziped_files) != list:
            list_unziped_files = [list_unziped_files]
        # looping through files in .zip and uploading to dataframe
        for nome_arquivo_xlsx in list_unziped_files:
            #   importing to dataframe
            reader = pd.read_excel(nome_arquivo_xlsx, encoding='utf-8')
            df_ = pd.DataFrame(reader)
            #   changing format to list of dictionaries
            list_banks_warranty_b3 = df_[list(df_.columns)[0]].tolist()
        # removing duplicates
        list_banks_warranty_b3 = HandlingLists().remove_duplicates(
            list_banks_warranty_b3)
        list_banks_warranty_b3 = [str(x).upper() for x in list_banks_warranty_b3]
        # creating dataframe with banks accepted as warranty in b3 clearing
        df_banks_accepted_warranty_b3 = pd.DataFrame([{col_issuers: bank} for bank in 
            list_banks_warranty_b3])
        df_banks_accepted_warranty_b3.sort_values([col_issuers], ascending=[True], inplace=True)
        # dealing with complete names from financial institutions
        df_banks_accepted_warranty_b3[col_issuers] = [StrHandler().remove_diacritics(
            StrHandler().replace_all(str(x).upper(), dict_replace_str_name).strip()) 
            for x in df_banks_accepted_warranty_b3[col_issuers].tolist()]
        # limiting banks accepted as warranty in b3 clearing
        df_banks_acc_warr = df_banks_operating_domestically[df_banks_operating_domestically[
            col_nome_extenso].isin(df_banks_accepted_warranty_b3[col_issuers])]
        # limitating columns of interest
        df_banks_acc_warr = df_banks_acc_warr[list_cols]
        # adding logging
        df_ = DBLogs().audit_log(
            df_, 
            DatesBR().utc_from_dt(DatesBR().curr_date), 
            DatesBR().utc_log_ts
        )
        # returning dataframe of inner join
        return df_banks_acc_warr
