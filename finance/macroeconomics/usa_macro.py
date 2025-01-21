
### LIBRARY TO COLLECT MACROECONOMIC INFORMATIONS FROM USA

import pandas as pd
from requests import request
from bs4 import BeautifulSoup
from stpstone.settings._global_slots import YAML_USA_MACRO
from stpstone.cals.handling_dates import DatesBR
from stpstone.handling_data.object import HandlingObjects
from stpstone.handling_data.html import HtmlHndler
from stpstone.handling_data.dicts import HandlingDicts
from stpstone.loggs.db_logs import DBLogs
from stpstone.handling_data.str import StrHandler


class USAMacro:

    def fred_request(self, api_key, data_source, data_source_id, date_inf=None, date_sup=None,
                     url_root='https://api.stlouisfed.org/fred/{}', file_type='json',
                     date_format=None):
        '''
        REFERENCES: https://fred.stlouisfed.org/docs/api/fred/
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        '''
        # adjusting variables types
        if date_format == 'dd/mm/yyyy':
            date_inf = DatesBR().str_date_to_datetime(date_inf)
            date_sup = DatesBR().str_date_to_datetime(date_sup)
        # query parameters for request: filetype / series id / realtime start / realtime end
        query_params = {
            'series_id': data_source_id,
            'file_type': file_type,
            'api_key': api_key,
            'realtime_start': date_inf,
            'realtime_end': date_sup
        }
        # return fred data
        message_content = request('GET', url_root.format(
            data_source), params=query_params).content
        return HandlingObjects().literal_eval_data(message_content, "b'", "'")

    @property
    def non_farm_payroll(self, list_td=list(), list_dicts_1=list(), list_dicts_2=list()):
        '''
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        '''
        # payload
        dict_payload = {}
        # header
        dict_headers = {
        'Referer': 'https://www.google.com/',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
        'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126", "Google Chrome";v="126"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'Cookie': 'ASP.NET_SessionId=dp3ziqlcjny45xqehrow1v2a; TEServer=TEIIS'
        }
        # requesting response
        resp_req = request(YAML_USA_MACRO['non_farm_payroll']['req_method'], 
                           YAML_USA_MACRO['non_farm_payroll']['url'], headers=dict_headers, 
                           data=dict_payload, 
                           verify=YAML_USA_MACRO['non_farm_payroll']['bl_verify'])
        # status code different from 2xx raise error
        resp_req.raise_for_status()
        # parsing html
        bs_html = BeautifulSoup(resp_req.content, 'html.parser')
        # * TABLE HISTORICAL DATA NON-FARM PAYROLL
        # headers
        list_th_0 = [
            td.get_text()
                .strip()
                .upper()
            for td in bs_html.find_all('th')
        ][:7]
        # data
        list_td1 = [
            td.get_text()
                .strip()
            for td in bs_html.find_all('td', class_='datatable-item datatable-item-positive')
        ]
        list_td2 = [
            td.get_text()
                .strip()
            for td in bs_html.find_all(
                'td', 
                style='text-align: left; padding-left: 10px; white-space: nowrap;'
            )
        ]
        list_td3 = [
            td.get_text()
                .strip()
            for td in bs_html.find_all('td', style='white-space: nowrap')
        ]
        list_td4 = [
            td.get_text()
                .strip()
            for td in bs_html.find_all('td', style='text-align: left')
        ]
        for i in range(len(list_td3)):
            list_td.extend([
                list_td2[i], list_td3[i], list_td4[i], list_td1[i+i*3], list_td1[i+1+i*3], 
                list_td1[i+2+i*3], list_td1[i+3+i*3]
            ])
        # creating list of dictionaries
        list_ser = HandlingDicts().pair_headers_with_data(
            list_th_0, 
            list_td
        )
        # serialized list into dataframe
        df_hist_nf_payroll = pd.DataFrame(list_ser)
        # * TABLE PAYROLLS AND USA LABOUR DATA
        for i in range(1, 3):
            #   table
            bs_table = bs_html.find_all('table')[i]
            #   headers
            if i == 1:
                list_th_1 = [
                    th.get_text()
                        .replace('\n', '')
                        .strip()
                        .upper() + str(i)
                    for th in bs_table.find_all('th')
                ]
            elif i == 2:
                list_th_2 = [
                    th.get_text()
                        .replace('\n', '')
                        .strip()
                        .upper() + str(i)
                    for th in bs_table.find_all('th')
                ]
            else:
                raise Exception('Table of payroll not supported for data treatment, ' 
                                + 'please revisit the parameter')
            #   data
            list_td = [
                td.get_text()
                    .replace('\n', '')
                    .strip()
                for td in bs_table.find_all('td')
            ]
            #   creating list of dictionaries
            if i == 1:
                list_dicts_1 = HandlingDicts().pair_headers_with_data(
                    list_th_1, 
                    list_td
                )
            elif i == 2:
                list_dicts_2 = HandlingDicts().pair_headers_with_data(
                    list_th_2, 
                    list_td
                )
                # print(list_dicts_2)
            else:
                raise Exception('Table of payroll not supported for data treatment, ' 
                                + 'please revisit the parameter')
        # serialized list into dataframe
        df_payrolls = pd.DataFrame(list_dicts_1)
        df_payrolls = df_payrolls[[c for c in list(df_payrolls.columns) 
                                   if (c not in list_th_0) and (c not in list_th_2)]]
        df_usa_labour_data = pd.DataFrame(list_dicts_2)
        df_usa_labour_data = df_usa_labour_data[[c for c in list(
            df_usa_labour_data.columns) if (c not in list_th_1) and (c not in list_th_0)]]
        # looping through dataframes
        for i, df_ in enumerate([
            df_payrolls, 
            df_usa_labour_data
        ]):
            #   dropping rows that are empty
            df_.dropna(subset=list(df_.columns), how='all', inplace=True)
            #   removing duplicates
            df_.drop_duplicates(inplace=True)
            #   renaming columns
            df_.rename(columns={
                x: StrHandler().get_string_until_substr(
                    x, str(i+1)) for x in list(df_.columns)
            }, inplace=True)
            #   adding logging
            df_ = DBLogs().audit_log(
                df_, 
                YAML_USA_MACRO['non_farm_payroll']['url'],
                DatesBR().utc_from_dt(DatesBR().curr_date)
            )
        # adding logging to the last dataframe
        df_hist_nf_payroll = DBLogs().audit_log(
            df_hist_nf_payroll, 
            YAML_USA_MACRO['non_farm_payroll']['url'],
            DatesBR().utc_from_dt(DatesBR().curr_date)
        )
        # returning dataframes
        return df_hist_nf_payroll, df_payrolls, df_usa_labour_data

# print(USAMacro().fred_request('ceaca22bd10bd67a93fca0a5cd4aa578', 'series', 'GNPCA'))
# # output
# {'realtime_start': '2020-12-07', 'realtime_end': '2020-12-07', 'seriess': [{'id': 'GNPCA', 'realtime_start': '2020-12-07', 'realtime_end': '2020-12-07', 'title': 'Real Gross National Product', 'observation_start': '1929-01-01', 'observation_end': '2019-01-01', 'frequency': 'Annual',
#                                                                             'frequency_short': 'A', 'units': 'Billions of Chained 2012 Dollars', 'units_short': 'Bil. of Chn. 2012 $', 'seasonal_adjustment': 'Not Seasonally Adjusted', 'seasonal_adjustment_short': 'NSA', 'last_updated': '2020-07-30 07:57:33-05', 'popularity': 13, 'notes': 'BEA Account Code: A001RX\\n\\n'}]}

# print(USAMacro().fred_request(
#     'ceaca22bd10bd67a93fca0a5cd4aa578', 'series', 'DGS10'))
