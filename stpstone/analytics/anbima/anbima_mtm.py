### ANBIMA MARKET TO MARKET ###

# pypi.org
import pandas as pd
from requests import request
from datetime import datetime
# local libs
from stpstone._config.global_slots import YAML_ANBIMA
from stpstone.utils.cals.handling_dates import DatesBR
from stpstone.utils.loggs.db_logs import DBLogs


class AnbimaMTM:

    def __init__(self, dt_ref:datetime=DatesBR().sub_working_days(DatesBR().curr_date, 1)) -> None:
        self.dt_ref = dt_ref

    def general_req(self, url:str, bl_verify:bool, method:str='GET', str_sep:str='@') -> pd.DataFrame:
        list_ser = list()
        req_resp = request(method, url, bl_verify)
        req_resp.raise_for_status()
        # turning content into list of rows
        str_tb = req_resp.text
        list_tb = str_tb.split('\r\n')[2:]
        # turn into serialized list
        for row in list_tb:
            #   reseting variables
            dict_ = dict()
            #   defining header
            if row.split(str_sep)[0] == 'Titulo':
                list_headers = row.split(str_sep)
            #   appending data
            else:
                #   creating serialized list
                dict_ = dict(zip(list_headers, row.replace(',', '.').split(str_sep)))
                #   appending to consolidated list
                list_ser.append(dict_)
        # turning into dataframe
        df_ = pd.DataFrame(list_ser)
        # droping nan values
        df_.dropna(inplace=True)
        # returning dataframe
        return df_

    @property
    def br_treasury_bonds(self):
        url = YAML_ANBIMA['sec_mkt_prcs']['br_treasuries']['url'].format(
            self.dt_ref.strftime('%y%m%d'))
        df_br_tb = self.general_req(url, YAML_ANBIMA['sec_mkt_prcs']['br_treasuries']['bl_verify'])
        list_headers = list(df_br_tb.columns)
        df_br_tb = df_br_tb.astype({
            list_headers[0]: str,
            list_headers[1]: int,
            list_headers[2]: int,
            list_headers[3]: int,
            list_headers[4]: int,
            list_headers[5]: float,
            list_headers[6]: float,
            list_headers[7]: float,
            list_headers[8]: float,
            list_headers[9]: float,
            list_headers[10]: float,
            list_headers[11]: float,
            list_headers[12]: float,
            list_headers[13]: float,
            list_headers[14]: str
        })
        df_br_tb = DBLogs().audit_log(
            df_br_tb,
            url,
            DatesBR().utc_from_dt(
                self.dt_ref
            )
        )
        return df_br_tb

    @property
    def corporate_bonds(self):
        url = YAML_ANBIMA['sec_mkt_prcs']['corporate_bonds']['url'].format(
            self.dt_ref.strftime('%y%m%d'))
        df_br_tb = self.general_req(url, YAML_ANBIMA['sec_mkt_prcs']['corporate_bonds']['bl_verify'])
        list_headers = list(df_br_tb.columns)
        df_br_tb = df_br_tb.astype({
            list_headers[0]: str, ## code
            list_headers[1]: str, ## name
            list_headers[2]: str, ## date
            list_headers[3]: str, ## maturity
            list_headers[4]: str, ## index
            list_headers[5]: float, ## bid rate
            list_headers[6]: float, ## ask rate
            list_headers[7]: float, ## market reference rate
            list_headers[8]: float, ## standard deviation
            list_headers[9]: float, ## indicative min range
            list_headers[10]: float, ## indicative max range
            list_headers[11]: float, ## present value (pv)
            list_headers[12]: float, ## pv @ par / VNE (future value - fv - or valor nominal
                                     ##     de emissão, pt-br)
            list_headers[13]: float, ## duration
        })
        df_br_tb = DBLogs().audit_log(
            df_br_tb,
            url,
            DatesBR().utc_from_dt(
                self.dt_ref
            )
        )
        return df_br_tb

    @property
    def ima(self):
        """
        DOCSTRING: MARKET INDEXES ANBIMA
        INPUTS:
        OUTPUTS:
        """
        dict_list_ser = dict()
        str_sep='@'
        req_resp = request(
            YAML_ANBIMA['sec_mkt_prcs']['ima']['method'],
            YAML_ANBIMA['sec_mkt_prcs']['ima']['url'],
            YAML_ANBIMA['sec_mkt_prcs']['ima']['bl_verify']
        )
        req_resp.raise_for_status()
        # turning content into list of rows
        str_tb = req_resp.text
        list_tb = str_tb.split('\r\n')[2:]
        # turn into serialized list
        for row in list_tb:
            #   table identifier
            str_tb_id = str(row.split(str_sep)[0])
            #   reseting variables
            dict_ = dict()
            #   if index dataframe not in dict of serialized lists, create a key
            if str_tb_id not in dict_list_ser:
                dict_list_ser[str_tb_id] = list()
            #   defining header
            if row.split(str_sep)[1] == 'Data de Referência':
                list_headers = row.split(str_sep)
            #   appending data
            else:
                #   creating serialized list
                dict_ = dict(zip(list_headers, row.replace(',', '.').split(str_sep)))
                #   appending to consolidated list
                dict_list_ser[str_tb_id].append(dict_)
        # turning into dataframes
        df_ima_pvs = pd.DataFrame(dict_list_ser['1'])
        df_ima_th_portf = pd.DataFrame(dict_list_ser['2'])
        # adding logging
        for df_ in [df_ima_pvs, df_ima_th_portf]:
            df_ = DBLogs().audit_log(
                df_,
                YAML_ANBIMA['sec_mkt_prcs']['ima']['url'],
                DatesBR().utc_from_dt(DatesBR().curr_date)
            )
        # returning dataframes
        return df_ima_pvs, df_ima_th_portf
