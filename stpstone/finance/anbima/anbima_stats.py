### ANBIMA STATISTICS ###

import pandas as pd
from math import nan
from pprint import pprint
from stpstone.settings._global_slots import YAML_ANBIMA
from stpstone.cals.handling_dates import DatesBR
from stpstone.handling_data.json import JsonFiles
from stpstone.handling_data.str import StrHandler
from stpstone.finance.comdinheiro.api_request import ComDinheiro
from stpstone.handling_data.object import HandlingObjects
from stpstone.handling_data.lists import HandlingLists
from stpstone.document_numbers.br import DocumentsNumbersBR
from stpstone.handling_data.dicts import HandlingDicts
from stpstone.handling_data.html import HtmlHndler
from stpstone.loggs.db_logs import DBLogs


class AnbimaStats:

    @property
    def pmi_hat(self):
        """
        DOCSTRING: IPCA PMI PROJECTED BY ANBIMA
        INPUTS: -
        OUTPUTS: DATAFRAME
        """
        # setting variables
        list_ser = list()
        # request html
        bs_html = HtmlHndler().html_bs_parser(
            YAML_ANBIMA['anbima_stats']['url_price_indexes'], 
            bl_verify=YAML_ANBIMA['anbima_stats']['bl_verify']
        )
        # looping within tables
        for i_tb, bs_table in enumerate(bs_html.find_all('table')):
            #   reseting variables
            list_data = list()
            list_headers = list()
            int_len_th = list()
            int_ref_tb = i_tb % 3
            int_ref_idx = i_tb//3
            #   defining current index
            if int_ref_idx == 0:
                str_idx = 'igpm'
            elif int_ref_idx == 1:
                str_idx = 'ipca'
            else:
                str_idx = 'others'
            #   defining the key of serialized list, accordingly to table name
            if int_ref_tb == 0:
                data_hat = YAML_ANBIMA['anbima_stats']['data_hat_mp1']
            elif int_ref_tb == 1:
                data_hat = YAML_ANBIMA['anbima_stats']['data_hat_mp2']
            elif int_ref_tb == 2:
                data_hat = YAML_ANBIMA['anbima_stats']['data_hat_l12m']
            #   looping within rows
            for i_tr, bs_tr in enumerate(bs_table.find_all('tr')):
                #   first row as header, others as 
                if i_tr == 1:
                    list_headers = [
                        StrHandler().remove_diacritics(x.get_text())\
                            .replace('*', '')\
                            .replace(' (%)', '')\
                            .replace(' da ', ' ')\
                            .replace(' de ', ' ')\
                            .replace(' ', '_')\
                            .replace('-', '')\
                            .upper()\
                            .replace(YAML_ANBIMA['anbima_stats']['col_ipca_eft'], 
                                     YAML_ANBIMA['anbima_stats']['col_pmi_eft'])\
                            .replace(YAML_ANBIMA['anbima_stats']['col_igpm_eft'], 
                                     YAML_ANBIMA['anbima_stats']['col_pmi_eft'])
                        for x in bs_tr.find_all('td')
                    ]
                    int_len_th = len(list_headers)
                    list_headers.extend([YAML_ANBIMA['anbima_stats']['col_pmi_core'], 
                                         YAML_ANBIMA['anbima_stats']['col_period_prediction']])
                elif i_tr > 0:
                    #   creating list to be appended in the consolidated data list
                    list_ = [
                        x.get_text().replace(',', '.')
                        for x in bs_tr.find_all('td')
                    ]
                    #   in case of length less than the headers, fill extremes with nan
                    if len(list_) < int_len_th:
                        list_.insert(0, nan)
                        list_.insert(len(list_headers)-1, nan)
                    #   adding pmi core
                    list_.extend([str_idx.upper(), data_hat.upper()])
                    #   extending to consolidated list
                    list_data.extend(list_)
            # creating serialized list
            list_ser = HandlingDicts().pair_headers_with_data(list_headers, list_data)
            # appending to exporting list
            list_ser.extend(list_ser)
        # importing to dataframe
        df_pmi_hat = pd.DataFrame(list_ser)
        # removing duplicates
        df_pmi_hat.drop_duplicates(inplace=True)
        # replacing values like nan
        df_pmi_hat.replace('-', nan, inplace=True)
        df_pmi_hat[YAML_ANBIMA['anbima_stats']['col_expirtation_dt']].fillna(
            YAML_ANBIMA['anbima_stats']['dt_fillna'], inplace=True)
        # adding request date
        df_pmi_hat[YAML_ANBIMA['anbima_stats']['col_request_date'].upper()] = \
            DatesBR().curr_date
        # altering data types
        for col_ in [
            YAML_ANBIMA['anbima_stats']['col_dt'], 
            YAML_ANBIMA['anbima_stats']['col_expirtation_dt']
        ]:
            df_pmi_hat[col_] = [DatesBR().str_date_to_datetime(x, 'DD/MM/YY') 
                                for x in df_pmi_hat[col_]]
        # sorting dataframe
        df_pmi_hat.sort_values([
            YAML_ANBIMA['anbima_stats']['col_pmi_core'], 
            YAML_ANBIMA['anbima_stats']['col_period_prediction'], 
            YAML_ANBIMA['anbima_stats']['col_dt']
        ], ascending=[False, True, False], inplace=True)
        # forward fill method (for nan values replace with last data, for the same column)
        df_pmi_hat = df_pmi_hat.groupby([
            YAML_ANBIMA['anbima_stats']['col_pmi_core'], 
            YAML_ANBIMA['anbima_stats']['col_period_prediction']
        ], as_index=False).apply(lambda group: group.ffill())
        df_pmi_hat.fillna(0, inplace=True)
        # altering data types
        df_pmi_hat = df_pmi_hat.astype({
            YAML_ANBIMA['anbima_stats']['col_month_collection']: str,
            YAML_ANBIMA['anbima_stats']['col_proj']: float,
            YAML_ANBIMA['anbima_stats']['col_pmi_core']: str,
            YAML_ANBIMA['anbima_stats']['col_period_prediction']: str,
            YAML_ANBIMA['anbima_stats']['col_month_proj']: str,
            YAML_ANBIMA['anbima_stats']['col_pmi_eft']: float
        })
        # creating logg
        df_pmi_hat = DBLogs().audit_log(
            df_pmi_hat, 
            YAML_ANBIMA['anbima_stats']['url_price_indexes'], 
            DatesBR().curr_date
        )
        # returning list
        return df_pmi_hat