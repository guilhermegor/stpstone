### ANBIMA DATA ###

import pandas as pd
from requests import request, exceptions
from pprint import pprint
from stpstone.handling_data.str import StrHandler
from stpstone.handling_data.json import JsonFiles
from stpstone.handling_data.dicts import HandlingDicts
from stpstone.cals.handling_dates import DatesBR
from stpstone.settings._global_slots import YAML_ANBIMA


class AnbimaDataGen:
    '''
    REFERENCES: https://developers.anbima.com.br/api-portal/pt-br
    '''

    def __init__(
            self,
            str_client_id: str, 
            str_client_secret: str, 
            str_env: str='dev',
            bl_debug: bool=False,
            int_chunk: int=1000,
            str_host_prd='https://api.anbima.com.br/',
            str_host_dev='https://api-sandbox.anbima.com.br/'
        ):
        '''
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        '''
        self.str_client_id = str_client_id
        self.str_client_secret = str_client_secret
        self.bl_debug = bl_debug
        self.int_chunk = int_chunk
        self.str_host_prd = str_host_prd
        self.str_host = locals()[f'str_host_{str_env.lower()}']
        self.str_token = self.access_token['access_token']

    @property
    def access_token(self, str_app='oauth/access-token', str_method='POST'):
        '''
        DOCSTRING: AUTHORIZATION TOKEN IN ORDER TO GRANT ACCESS TO DESIRED APP
        INPUTS: STR_APP, STR_METHOD
        OUTPUTS: JSON
        '''
        # url
        str_url = self.str_host_prd + str_app
        # encoding credentials to base64
        base64_credentials = StrHandler().base64_encode(
            self.str_client_id, 
            self.str_client_secret
        )
        # request
        dict_headers = {
            'Content-Type': 'application/json',
            'Authorization': base64_credentials
        }
        dict_payload = {
            'grant_type': 'client_credentials'
        }
        resp_req = request(
            method=str_method, 
            url=str_url, 
            headers=dict_headers, 
            data=JsonFiles().dict_to_json(dict_payload)
        )
        if self.bl_debug == True:
            print('TOKEN: {}'.format(resp_req.json()['access_token']))
        # checking wheter response code is 2xx, otherwise raise error
        resp_req.raise_for_status()
        # returning token
        return resp_req.json()

    def generic_request(self, str_app, str_method):
        '''
        DOCSTRING: GENERIC REQUEST TO ANBIMA DATA
        INPUTS: STR_APP, STR_METHOD, DICT_PAYLOAD
        OUTPUTS: JSON
        '''
        # url
        str_url = self.str_host + str_app
        if self.bl_debug == True:
            print(f'URL: {str_url}')
        # request
        dict_headers = {
            'accept': 'application/json',
            'client_id': self.str_client_id,
            'access_token': self.str_token
        }
        resp_req = request(
            method=str_method, 
            url=str_url, 
            headers=dict_headers
        )
        # checking wheter response code is 2xx, otherwise raise error
        resp_req.raise_for_status()
        # returning token
        return resp_req.json()


class AnbimaDataFunds(AnbimaDataGen):
    '''
    REFERENCES: https://developers.anbima.com.br/pt/swagger-de-fundos-v2-rcvm-175/#/Notas%20explicativas/buscarNotasExplicativas
    '''

    def funds_raw(
        self,
        int_pg=None,
        str_app='feed/fundos/v2/fundos?size={}&page={}',
        str_method='GET'
    ):
        '''
        DOCSTRING: RETRIEVE AVAILABLE CLOSED AND OPENED-END FUNDS
        INPTUS: -
        OUTPUTS: JSON
        '''
        return self.generic_request(
            str_app.format(
                self.int_chunk,
                int_pg,
            ), 
            str_method
        )
    
    def funds_trt(self, i_pg=0, i_fnd=0, list_ser=list()):
        '''
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        '''
        # looping within all available fund pages
        while True:
            #   requesting for current fund json, in case status code is different
            #       from 2xx, break the loop and return the dataframe
            try:
                json_funds = self.funds_raw(i_pg)
            except exceptions.HTTPError:
                break
            # looping within content dictionaries
            for dict_cnt in json_funds[YAML_ANBIMA['anbima_data_api']['key_content']]:
                #   setting variables
                dict_aux = dict()
                i_fnd += 1
                #   looping within keys and values from content
                for YAML_ANBIMA['anbima_data_api']['key_cnt'], data_cnt in dict_cnt.items():
                    if isinstance(data_cnt, str):
                        dict_aux[YAML_ANBIMA['anbima_data_api']['key_cnt']] = data_cnt.strip()
                    elif data_cnt is None:
                        dict_aux[YAML_ANBIMA['anbima_data_api']['key_cnt']] = data_cnt
                    elif isinstance(data_cnt, list):
                        #   looping within classes
                        for i_cls, dict_cls in enumerate(data_cnt):
                            #   looping within classes and appending to serialized list
                            for YAML_ANBIMA['anbima_data_api']['key_cls'], data_cls in dict_cls.items():
                                if \
                                    (YAML_ANBIMA['anbima_data_api']['key_cls'] != YAML_ANBIMA['anbima_data_api']['key_name_sbclss'])\
                                    and (data_cls is not None):
                                    dict_aux[YAML_ANBIMA['anbima_data_api']['key_cls']] = data_cls.strip()
                                elif \
                                    (YAML_ANBIMA['anbima_data_api']['key_cls'] != YAML_ANBIMA['anbima_data_api']['key_name_sbclss']) \
                                    and (data_cls is None):
                                    dict_aux[YAML_ANBIMA['anbima_data_api']['key_cls']] = data_cls
                                elif \
                                    (YAML_ANBIMA['anbima_data_api']['key_cls'] == YAML_ANBIMA['anbima_data_api']['key_name_sbclss']) \
                                    and (isinstance(data_cls, list)):
                                    #   looping within subclasses and copy auxiliary dicitionary in each iteration, 
                                    #       in order to renew the subclass info imported
                                    for dict_sbcls in data_cls:
                                        dict_xpt = dict_aux.copy()
                                        for YAML_ANBIMA['anbima_data_api']['key_sbcls'], data_sbcls in dict_sbcls.items():
                                            dict_xpt = HandlingDicts().merge_n_dicts(
                                                dict_xpt,
                                                {
                                                    '{}_{}'.format(
                                                        YAML_ANBIMA['anbima_data_api']['key_name_sbcls'],
                                                        YAML_ANBIMA['anbima_data_api']['key_sbcls']
                                                    ): data_sbcls
                                                },
                                                {
                                                    YAML_ANBIMA['anbima_data_api']['col_num_fnd']: i_fnd + 1,
                                                    YAML_ANBIMA['anbima_data_api']['col_num_class']: i_cls + 1,
                                                    YAML_ANBIMA['anbima_data_api']['col_num_pg']: i_pg
                                                }
                                            )
                                        list_ser.append(dict_xpt)
                                elif \
                                    (YAML_ANBIMA['anbima_data_api']['key_cls'] == YAML_ANBIMA['anbima_data_api']['key_name_sbclss'])\
                                    and (data_cls is None):
                                    list_ser.append(
                                        HandlingDicts().merge_n_dicts(
                                            dict_aux,
                                            {YAML_ANBIMA['anbima_data_api']['key_cls']: data_cls},
                                            {
                                                YAML_ANBIMA['anbima_data_api']['col_num_fnd']: i_fnd + 1,
                                                YAML_ANBIMA['anbima_data_api']['col_num_class']: i_cls + 1,
                                                YAML_ANBIMA['anbima_data_api']['col_num_pg']: i_pg
                                            }
                                        )
                                    )
                                else:
                                    raise Exception('Error of content within class, please revise ' \
                                        + 'pg: {} / key: {} / data: {}'.format(
                                            i_pg,
                                            YAML_ANBIMA['anbima_data_api']['key_cls'],
                                            data_cls
                                        ))
                    else:
                        raise Exception(f'Error of content data type, please revise the data: {data_cnt}')
            #   adding iterator
            i_pg += 1
        # appending serialized list to pandas dataframe
        df_funds = pd.DataFrame(list_ser)
        # changing columns types
        for col_dt in [
            YAML_ANBIMA['anbima_data_api']['col_fund_closure_dt'],
            YAML_ANBIMA['anbima_data_api']['col_eff_dt'],
            YAML_ANBIMA['anbima_data_api']['col_incpt_dt'],
            YAML_ANBIMA['anbima_data_api']['col_closure_dt'],
            YAML_ANBIMA['anbima_data_api']['col_sbc_incpt_dt'],
            YAML_ANBIMA['anbima_data_api']['col_sbc_closure_dt'],
            YAML_ANBIMA['anbima_data_api']['col_sbc_eff_dt']
        ]:
            df_funds[col_dt].fillna(YAML_ANBIMA['anbima_data_api']['str_dt_fill_na'], inplace=True)
            df_funds[col_dt] = [
                DatesBR().str_date_to_datetime(d, YAML_ANBIMA['anbima_data_api']['str_dt_format']) 
                for d in df_funds[col_dt]
            ]
        for col_dt in [
            YAML_ANBIMA['anbima_data_api']['col_update_ts'],
            YAML_ANBIMA['anbima_data_api']['col_sbc_update_dt']
        ]:
            df_funds[col_dt].fillna(YAML_ANBIMA['anbima_data_api']['str_ts_fill_na'], inplace=True)
            df_funds[col_dt] = [
                DatesBR().timestamp_separator_string_to_datetime(d, format=YAML_ANBIMA['anbima_data_api']['str_dt_format']) 
                for d in df_funds[col_dt]
            ]
        df_funds.fillna(YAML_ANBIMA['anbima_data_api']['str_fill_na'], inplace=True)
        df_funds = df_funds.astype({
            YAML_ANBIMA['anbima_data_api']['col_fund_code']: str,
            YAML_ANBIMA['anbima_data_api']['col_type_id']: str,
            YAML_ANBIMA['anbima_data_api']['col_fund_id']: str,
            YAML_ANBIMA['anbima_data_api']['col_comp_name']: str,
            YAML_ANBIMA['anbima_data_api']['col_trade_name']: str,
            YAML_ANBIMA['anbima_data_api']['col_fund_type']: str,
            YAML_ANBIMA['anbima_data_api']['col_class_code']: str,
            YAML_ANBIMA['anbima_data_api']['col_class_id_type']: str,
            YAML_ANBIMA['anbima_data_api']['col_class_id']: str,
            YAML_ANBIMA['anbima_data_api']['col_comp_class']: str,
            YAML_ANBIMA['anbima_data_api']['col_trd_class']: str,
            YAML_ANBIMA['anbima_data_api']['col_n1_ctg']: str,
            YAML_ANBIMA['anbima_data_api']['col_subclasses']: str,
        })
        # removing duplicates
        df_funds.drop_duplicates(inplace=True)
        # returning dataframe
        return df_funds

    def fund_raw(
        self, 
        str_code_fnd: str,
        str_app='feed/fundos/v2/fundos/{}/historico',
        str_method='GET'
    ):
        '''
        DOCSTRING: RETRIEVE INFORMATION REGARDING A SPECIFIC FUND
        INPTUS: CODIGO_FUNDO FROM ANBIMA DATA INFOS REGARDING THE CHOSEN FUND
        OUTPUTS: JSON
        '''
        return self.generic_request(str_app.format(str_code_fnd), str_method)
    
    def fund_trt(
        self,
        list_code_fnds: list,
        dict_dfs=dict()
    ):
        '''
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        '''
        # looping within the funds codes
        for str_code_fnd in list_code_fnds:
            #   setting variables
            dict_aux = dict()
            list_ser = list()
            dict_dfs[str_code_fnd] = list()
            #   returning fund info
            json_fnd_info = self.fund_raw(str_code_fnd)
            #   looping within json content
            for key_cnt, data_cnt in json_fnd_info.items():
                #   checking data type, when is a list create the dictionary for the 
                #       serialized list to be appended into a pandas df
                if (isinstance(data_cnt, str) or data_cnt is None):
                    dict_aux[key_cnt] = data_cnt
                elif isinstance(data_cnt, list):
                    dict_xpt = dict_aux.copy()
                    for dict_data in data_cnt:
                        for key_data, data_data in dict_data.items():
                            #   checking wheter the data instance is string, or list
                            if (isinstance(data_data, str) or data_data is None):
                                dict_xpt['{}_{}'.format(
                                    key_cnt,
                                    key_data
                                )] = data_data
                            elif isinstance(data_data, list):
                                for dict_hist in data_data:
                                    dict_xpt_2 = dict_xpt.copy()
                                    for key_hist, data_hist in dict_hist.items():
                                        dict_xpt_2['{}_{}_{}'.format(
                                            key_cnt,
                                            key_data,
                                            key_hist
                                        )] = data_hist
                                    list_ser.append(dict_xpt_2)
                        #   regarding classes first-order dictionary has a key called historico_classe, which is 
                        #       a list (in 2024-11-08), it is treated separately in the code, in order to create the 
                        #       serialized list
                        if key_data != 'classes':
                            list_ser.append(dict_xpt)
                    df_ = pd.DataFrame(list_ser)
                    #   changing data types within columns
                    for col_ in list(df_.columns):
                        #   date
                        if \
                            (StrHandler().match_string_like(col_,'*data_*') == True) \
                            and (len(col_) == 10):
                            df_[col_].fillna(YAML_ANBIMA['anbima_data_api']['str_dt_fill_na'], inplace=True)
                            df_[col_] = [
                                DatesBR().str_date_to_datetime(d, YAML_ANBIMA['anbima_data_api']['str_dt_format']) 
                                for d in df_[col_]
                            ]
                        #   timestamp
                        elif \
                            (StrHandler().match_string_like(col_,'*data_*') == True) \
                            and (StrHandler().match_string_like(col_,'*T*') == True) \
                            and (len(col_) > 10):
                            df_[col_].fillna(YAML_ANBIMA['anbima_data_api']['str_ts_fill_na'], inplace=True)
                            df_[col_] = [
                                DatesBR().timestamp_separator_string_to_datetime(
                                    d, format=YAML_ANBIMA['anbima_data_api']['str_dt_format']
                                ) 
                                for d in df_[col_]
                            ]
                        #   float
                        elif \
                            (StrHandler().match_string_like(col_,'*percentual_*') == True):
                            df_[col_].fillna(YAML_ANBIMA['anbima_data_api']['str_float_fill_na'], inplace=True)
                            df_[col_] = [float(x) for x in df_[col_]]
                        #   string
                        else:
                            df_[col_].fillna(YAML_ANBIMA['anbima_data_api']['str_fill_na'], inplace=True)
                            df_[col_] = [str(x).strip() for x in df_[col_]]
                    #   appending to list of dataframe
                    dict_dfs[str_code_fnd].append(df_)
                else:
                    raise Exception('Type of data within the content of the fund {} not found, please check the code. '.format(
                        str_code_fnd
                    ) + 'TYPE: {}'.format(type(data_cnt)))
        # returning list of dataframes with fund infos
        return dict_dfs

    def fund_hist(
            self, 
            str_code_class: str,
            str_app='feed/fundos/v2/fundos/{}/historico',
            str_method='GET'
        ):
        '''
        DOCSTRING: HISTORICAL CHANGES OF REGISTRATION DATA FOR THE FUND
        INPTUS: CODIGO_FUNDO FROM ANBIMA DATA INFOS REGARDING THE CHOSEN FUND
        OUTPUTS: JSON
        '''
        return self.generic_request(str_app.format(str_code_class), str_method)
    
    def segment_investor(
            self, 
            str_code_class: str,
            str_app='feed/fundos/v2/fundos/segmento-investidor/{}/patrimonio-liquido',
            str_method='GET'
        ):
        '''
        DOCSTRING: INVESTOR SEGMENT INFORMATION, DISPLAYING THE DISTRIBUTION OF AUM 
            BY PERCENTAGE VALUES
        INPTUS: CODIGO_FUNDO FROM ANBIMA DATA INFOS REGARDING THE CHOSEN FUND
        OUTPUTS: JSON
        '''
        return self.generic_request(str_app.format(str_code_class), str_method)
    
    def time_series_fund(
            self,
            str_date_inf: str,
            str_date_sup: str,
            str_code_class: str,
            str_app='feed/fundos/v2/fundos/{}/serie-historica',
            str_method='GET'
        ):
        '''
        DOCSTRING: TIME SERIES FOR A GIVEN FUND
        INPTUS: CODIGO_FUNDO FROM ANBIMA DATA INFOS REGARDING THE CHOSEN FUND
        OUTPUTS: JSON
        '''
        dict_payload = {
            'size': self.int_chunk,
            'data-inicio': str_date_inf,
            'data-fim': str_date_sup
        }
        return self.generic_request(
            str_app.format(str_code_class, self.int_chunk), 
            str_method=str_method, 
            dict_payload=dict_payload
        )
    
    def funds_financials_dt(
            self,
            str_date_update: str,
            str_app='feed/fundos/v2/fundos/serie-historica/lote',
            str_method='GET'
        ):
        '''
        DOCSTRING: TIME SERIES OF FUNDS FOR A GIVEN DATE OF UPDATE
        INPTUS: CODIGO_FUNDO FROM ANBIMA DATA INFOS REGARDING THE CHOSEN FUND
        OUTPUTS: JSON
        '''
        dict_payload = {
            'data-atualizacao': str_date_update,
            'size': self.int_chunk
        }
        return self.generic_request(
            str_app, 
            str_method=str_method, 
            dict_payload=dict_payload
        )

    def funds_registration_data_dt(
            self,
            str_date_update: str,
            str_app='feed/fundos/v2/fundos/dados-cadastrais/lote',
            str_method='GET'
        ):
        '''
        DOCSTRING: FUNDS REGISTRATION FOR A GIVEN DATE
        INPTUS: DATE OF UPDATE
        OUTPUTS: JSON
        '''
        dict_payload = {
            'data-atualizacao': str_date_update,
            'size': self.int_chunk
        }
        return self.generic_request(
            str_app, 
            str_method=str_method, 
            dict_payload=dict_payload
        )
    
    @property
    def institutions(
            self,
            str_app='feed/fundos/v2/fundos/instituicoes',
            str_method='GET'
        ):
        '''
        DOCSTRING: INSTITUTIONS MANAGING CLOSED/OPENED-END FUNDS
        INPTUS: -
        OUTPUTS: JSON
        '''
        dict_payload = {
            'size': self.int_chunk
        }
        return self.generic_request(
            str_app, 
            str_method=str_method, 
            dict_payload=dict_payload
        )
    
    def institution(
            self,
            str_ein: str,
            str_app='feed/fundos/v2/fundos/instituicoes/{}',
            str_method='GET'
        ):
        '''
        DOCSTRING: INSTITUTIONS MANAGING CLOSED/OPENED-END FUNDS
        INPTUS: EIN (EMPLOYER IDENTIFICATION NUMBER)
        OUTPUTS: JSON
        '''
        dict_payload = {
            'size': self.int_chunk
        }
        return self.generic_request(
            str_app.format(str_ein), 
            str_method=str_method, 
            dict_payload=dict_payload
        )
    
    def explanatory_notes_fund(
            self, 
            str_code_class: str,
            str_app='feed/fundos/v2/fundos/{}/notas-explicativas',
            str_method='GET'
        ):
        '''
        DOCSTRING: EXPLANATORY NOTES FOR A GIVEN FUND
        INPTUS: CODIGO_FUNDO FROM ANBIMA DATA INFOS REGARDING THE CHOSEN FUND
        OUTPUTS: JSON
        '''
        return self.generic_request(str_app.format(str_code_class), str_method)