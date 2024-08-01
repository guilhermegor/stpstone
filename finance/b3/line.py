### API TO GRANT ACCESS TO PRE TRADING LINE B3 SOLUTIONS ###

import time
import pandas as pd
from requests import request
from pprint import pprint
from stpstone.handling_data.json import JsonFiles
from stpstone.cals.handling_dates import DatesBR


class ConnectionApi:
    '''
    REFENCES: http://www.b3.com.br/data/files/2E/95/28/F1/EBD17610515A8076AC094EA8/GUIDE-TO-LINE-5.0-API.pdf,
        https://line.bvmfnet.com.br/#/endpoints
    IMPORTANT: REQUIRE PROFILE LINE_SVC-FIRM-MNG-RIS_BVMF TO THE CAU EXCHANGE MANAGER ASSOCIATATED TO
        THE SERVICE USER
    '''

    def __init__(self, client_id, client_secret, broker_code, category_code, token=None,
                 hostname_api_line_b3='https://api.line.bvmfnet.com.br'):
        self.client_id = client_id
        self.client_secret = client_secret
        self.broker_code = broker_code
        self.category_code = category_code
        self.token = token
        self.hostname_api_line_b3 = hostname_api_line_b3

    @property
    def auth_header(self, method='GET', key_header='header', int_max_retrieves=1000,
                    int_status_code_ok=200, int_status_code_iteration=400, bl_verify=False,
                    app='/api/v1.0/token/authorization'):
        '''
        DOCSTRING: AUTHENTICATION HEADER TO LINE B3 API
        INPUTS: METHOD (DEFAULT), KEY HEADER (DEFAULT), URL AUTHENTIFICATION
            HEADER (DEFAULT)
        OUTPUTS: STRING
        '''
        # passing variables
        i = 0
        # requesting authorization authheader
        dict_headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/x-www-form-urlencoded',
        }
        # looping while status code not a 2xx response
        while (int_status_code_iteration != int_status_code_ok) and (
                i <= int_max_retrieves):
            try:
                resp_req = request(method=method, url=self.hostname_api_line_b3 + app,
                                   headers=dict_headers, verify=bl_verify)
            except:
                continue
            int_status_code_iteration = resp_req.status_code
            i += 1
        # raises exception when not a 2xx response
        resp_req.raise_for_status()
        # getting authheader
        return resp_req.json()[key_header]

    @property
    def access_token(self, method='POST', int_refresh_min_time=4000,
                     max_retrieves=100, int_status_code_ok=200,
                     int_status_code_iteration=400, key_refresh_token='refresh_token',
                     key_access_token='access_token',
                     key_expires_in='expires_in', bl_str_dict_params=False,
                     bl_verify=False, i_retrieves=0,
                     i_aux=0, int_expiration_time=0, app='/api/oauth/token'):
        '''
        DOCSTRING: TOKEN TO GRANT ACCESS TO LINE BVMF SERVER
        INPUTS: METHOD (DEFAULT), MININUM TIME TO REFRESH (DEFAULT),
            MAXIMUM RETRIEVES (DEFAULT), KEY REFRESH TOKEN (DEFAULT),
            KEY ACCESS TOKEN (DEFAULT), KEY EXPIRATION TIME (DEFAULT), BOOLEAN STRING PARAMETERS (
            DEFAULT), CURRENT NUMBER OF RETRIEVES (DEFAULT), EXPIRATION TIME (DEFAULT),
            APP REFRESH TOKEN (DEFAULT)
        OUTPUTS: STRING
        '''
        # header
        dict_headers = {
            'Authorization': 'Basic {}'.format(self.auth_header),
        }
        # if expiration time is inferior to base time, trigger a refresh code
        while (int_expiration_time < int_refresh_min_time) and (i_retrieves < max_retrieves):
            #   dict_params with grant type, username, password broker code and category code -
            #       if its the first retrieve use a different dict_params dictionary
            if i_retrieves == 0:
                dict_params = {
                    'grant_type': 'password',
                    'username': str(self.client_id),
                    'password': str(self.client_secret),
                    'brokerCode': str(self.broker_code),
                    'categoryCode': str(self.category_code)
                }
            else:
                dict_params = {
                    'grant_type': 'refresh_token',
                    'refresh_token': refresh_token
                }
            #   coverting to string, if its user will
            if bl_str_dict_params == True:
                dict_params = '&'.join('{}={}'.format(k, v)
                                       for k, v in dict_params.items())
            # looping while status code not a 2xx response
            while (int_status_code_iteration != int_status_code_ok) and (
                    i_aux <= max_retrieves):
                try:
                    resp_req = request(method=method, url=self.hostname_api_line_b3 + app,
                                       headers=dict_headers, params=dict_params, verify=bl_verify)
                except:
                    continue
                int_status_code_iteration = resp_req.status_code
                i_aux += 1
            #   raises exception when not a 2xx response
            resp_req.raise_for_status()
            #   retrieving json
            dict_token = resp_req.json()
            #   refresh token
            refresh_token = dict_token[key_refresh_token]
            #   token
            token = dict_token[key_access_token]
            #   expiration time
            int_expiration_time = dict_token[key_expires_in]
            #   iterating through number of retrieves
            i_retrieves += 1
        # return token to requester
        return token

    def app_request(self, token, method, app_line_b3, dict_params=None, dict_payload=None,
                    bl_parse_dict_params_data=False, bl_retry_if_error=False, bl_retry_request=True,
                    bl_debug_mode=False, int_max_retrieves=100, float_secs_sleep=None,
                    float_secs_sleep_increase_error=1.0, int_status_code_ok=200,
                    list_int_http_error_token=[401]):
        '''
        DOCSTRING: REQUISITION FROM LINE B3 APPS
        INPUTS: METHOD, APP LINE B3, DICT PARAMS
        OUTPUTS: JSON
        '''
        # passing variables
        i = 0
        float_secs_sleep_iteration = float_secs_sleep
        # header
        dict_header = {
            'Authorization': 'Bearer {}'.format(token),
            'Content-Type': 'application/json'
        }
        # check wheter it is needed to parse the params dictionary
        if bl_parse_dict_params_data == True:
            if dict_params != None:
                dict_params = JsonFiles().dict_to_json(dict_params)
            if dict_payload != None:
                dict_payload = JsonFiles().dict_to_json(dict_payload)
        # request instrument informations - id, symbol and asset
        if bl_retry_if_error == True:
            while (bl_retry_request == True) and (i <= int_max_retrieves):
                if bl_debug_mode == True:
                    print('*** ATTEMPT REQUEST #{} ***'.format(i))
                # # request
                # print('URL: {}'.format(self.hostname_api_line_b3 + app_line_b3))
                # print('PARAMS: {}'.format(dict_params))
                # print('DATA: {}'.format(dict_payload))
                try:
                    resp_req = request(method=method, url=self.hostname_api_line_b3 + app_line_b3,
                                       headers=dict_header, params=dict_params, data=dict_payload)
                    # print('ENDPOINT + API: {}'.format(resp_req.url))
                    if resp_req.status_code == int_status_code_ok:
                        bl_retry_request = False
                    elif resp_req.status_code in list_int_http_error_token:
                        #   reset token wheter http error 401 has been reached
                        token = self.access_token
                        dict_header = {
                            'Authorization': 'Bearer {}'.format(token),
                            'Content-Type': 'application/json'
                        }
                    else:
                        float_secs_sleep_iteration += float_secs_sleep_increase_error
                    if bl_debug_mode == True:
                        print('REQUEST SUCCESFULLY MADE')
                except:
                    bl_retry_request = True
                    if bl_debug_mode == True:
                        print('EXCEPTION IN REQUEST #{}'.format(i))
                # wait
                if float_secs_sleep_iteration != None:
                    time.sleep(float_secs_sleep_iteration)
                # iteration increase
                i += 1
            # reseting variables
            float_secs_sleep_iteration = float_secs_sleep
        else:
            resp_req = request(method=method, url=self.hostname_api_line_b3 + app_line_b3,
                               headers=dict_header, params=dict_params, data=dict_payload)
            if bl_debug_mode == True:
                print('REQUEST SUCCESFULLY MADE')
        #   raises exception when not a 2xx response
        resp_req.raise_for_status()
        #   retrieving response
        try:
            return resp_req.json()
        except:
            return resp_req.status_code


class Operations(ConnectionApi):

    @property
    def exchange_limits(self, method='GET',
                        app='/api/v1.0/exchangeLimits/spxi/{}', bl_retry_if_error=True):
        '''
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        '''
        return self.app_request(self.token, method, app.format(self.broker_code),
                                bl_retry_if_error=bl_retry_if_error)

    @property
    def groups_authorized_markets(self, method='GET',
                                  app='/api/v1.0/exchangeLimits/autorizedMarkets',
                                  bl_retry_if_error=True):
        '''
        DOCSTRING: COLLECT AUTHORIZED MARKETS
        INPUTS: METHOD (DEFAULT GET), APP (DEFAULT), AND BOOLEAN RETRY IF ERROR (TRUE AS DEFAULT)
        OUTPUTS:
        '''
        return self.app_request(self.token, method, app, bl_retry_if_error=bl_retry_if_error)

    def intruments_per_group(self, group_id, method='POST', str_bl_settled='true',
                             bl_parse_dict_params_data=True, float_secs_sleep=None,
                             app='/api/v1.0/exchangeLimits/findInstruments'):
        '''
        DOCSTRING: SPXI INCLUSION TO DOCUMENT
        INPUTS: DOCUMENT ID, DICTIONARY PAYLOAD (LIST OF DICTIONARIES WITH KEYS: 'instrumentId': int,
            'isRemoved': 'false', 'spci': int, 'spvi': int, 'symbol': str), BOOLEAN PARSE DICT,
            METHOD (DEFAULT), APP SPXI (DEFAULT)
        OUTPUTS: STATUS CODE
        '''
        dict_payload = {
            'authorizedMarketGroupId': group_id,
            'isLimitSetted': str_bl_settled
        }
        return self.app_request(self.token, method, app, dict_payload=dict_payload,
                                bl_parse_dict_params_data=bl_parse_dict_params_data,
                                float_secs_sleep=float_secs_sleep)

    @property
    def authorized_markets_instruments(self, key_id='id', key_name='name',
                                       key_assets_associated='assets_associated',
                                       key_limit_spci_opt='limitSpciOption',
                                       key_limit_spvi_opt='limitSpviOption',
                                       key_limit_spci='limitSpci',
                                       key_limit_spvi='limitSpvi',
                                       key_instrument_symbol='instrumentSymbol',
                                       key_instrument_asset='instrumentAsset',
                                       dict_export=dict()):
        '''
        DOCSTRING: DICTIONARY WITH INSTRUMENTS AND RESPECTIVE AUTHORIZED MARKETS
        INPUTS: -
        OUTPUTS: DICTIONARY
        '''
        # json groups of authorized markets
        json_authorized_markets = self.groups_authorized_markets
        # loop through each authorized market and collect its assets associated
        for dict_ in json_authorized_markets:
            for dict_assets in self.intruments_per_group(dict_[key_id]):
                # print(dict_assets['instrumentSymbol'])
                #   in case the profile id is not in the exporting dict, include its as a key
                if dict_[key_id] not in dict_export:
                    dict_export[dict_[key_id]] = dict()
                #   in case it is already a key, include id, name, and assets associated in the
                #       values as a new dictionary nested
                else:
                    dict_export[dict_[key_id]][key_id] = dict_[key_id]
                    dict_export[dict_[key_id]][key_name] = dict_[key_name]
                    #   check wheter the assets associated key already exists or not, create a list if
                    #       it is not present, and appending a dictionary with symbol, asset, spci,
                    #       spvi, spci option and spvi option limits otherwise
                    if key_assets_associated not in dict_export[dict_[key_id]]:
                        dict_export[dict_[key_id]
                                    ][key_assets_associated] = list()
                    else:
                        if key_limit_spci_opt in dict_assets:
                            dict_export[dict_[key_id]][key_assets_associated].append({
                                'instrument_symbol': dict_assets[key_instrument_symbol],
                                'instrument_asset': dict_assets[key_instrument_asset],
                                'limit_spci': dict_assets[key_limit_spci],
                                'limit_spvi': dict_assets[key_limit_spvi],
                                'limit_spci_option': dict_assets[key_limit_spci_opt],
                                'limit_spvi_option': dict_assets[key_limit_spvi_opt],
                            })
                        else:
                            dict_export[dict_[key_id]][key_assets_associated].append({
                                'instrument_symbol': dict_assets[key_instrument_symbol],
                                'instrument_asset': dict_assets[key_instrument_asset],
                                'limit_spci': dict_assets[key_limit_spci],
                                'limit_spvi': dict_assets[key_limit_spvi]
                            })
        #  return dictionary with ticker, id of authorized market, maximum spxi and maximum option
        #       spxi, in case it is available
        return dict_export


class Resources(Operations):

    @property
    def instrument_informations(self, method='GET',
                                app='/api/v1.0/symbol', bl_retry_if_error=True):
        '''
        DOCSTRING: INSTRUMENTS INFORMATION
        INPUTS: MEHTOD (DEFAULT), APP INSTRUMENT INFORMATION (DEFAULT)
        OUTPUTS: JSON (ID, SYMBOL AND ASSET)
        '''
        return self.app_request(self.token, method, app,
                                bl_retry_if_error=bl_retry_if_error)

    @property
    def instrument_infos_exchange_limits(self, key_infos_id='id',
                                         key_exchange_limits_id='instrumentId', key_symbol='symbol'):
        '''
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        '''
        # dataframe of exchange limits
        df_exchange_limits = pd.DataFrame.from_dict(self.exchange_limits)
        # convert data types
        df_exchange_limits = df_exchange_limits.astype({
            key_exchange_limits_id: str
        })
        # dataframe of instrument informations
        df_instrument_informations = pd.DataFrame.from_dict(
            self.instrument_informations)
        # convert data types
        df_instrument_informations = df_instrument_informations.astype({
            key_infos_id: str
        })
        # left join instrument infos and exchange limits
        df_join_instruments = df_instrument_informations.merge(df_exchange_limits, how='left',
                                                               left_on=key_infos_id,
                                                               right_on=key_exchange_limits_id)
        # rename columns of interest
        df_join_instruments = df_join_instruments.rename(columns={
            key_symbol + '_x': key_symbol
        })
        # remove columns of interest
        df_join_instruments.drop(columns=[key_symbol + '_y'], inplace=True)
        # exporting dictionary with symbol as key
        return {row[key_symbol]: {col_: row[col_] for col_ in df_join_instruments.columns}
                for _, row in df_join_instruments.iterrows()}

    def instrument_id_by_symbol(self, symbol, method='GET', app='/api/v1.0/symbol/{}'):
        '''
        DOCSTRING: RETRIEVES INSTRUMENT ID BY ITS SYMBOL
        INPUTS: INSTRUMENT SYMBOL (STR)
        OUTPUTS: INSTRUMENT ID (INT)
        '''
        return self.app_request(self.token, method, app.format(str(symbol)))


class AccountsData(ConnectionApi):

    def client_infos(self, account_code, bl_retry_if_error=True, bl_debug_mode=False,
                     float_secs_sleep=None, method='GET', app='/api/v1.0/account'):
        '''
        DOCSTRING: CLIENT REGISTER ON LINE B3dict_id_ativos_interesse
        INPUTS: ACCOUNT CODE, METHOD (DEFAULT), APP CLIENT INFOS (DEFAULT)
        OUTPUTS: JSON - KEYS: INTERNAL ID, CODE, DOCUMENT, BOOLEAN PROTECTED, BOOLEAN BLOCKED,
            NAME, CATEGORY, SEGMENT, STATUS, PARTICIPANT NAME, PNP NAME, PARTICIPANT CODE,
            PNP CODE, PNP ACCOUNT CODE, PARTICIPANT ACCOUNT CODE, PARTICIPANT ACCOUNT CODE,
            ACCOUNT TYPE, OWNER DOCUMENT CODE, OWNER NAME
        '''
        # parameters
        dict_params = {
            'participantCode': self.broker_code,
            'pnpCode': self.category_code,
            'accountCode': account_code,
        }
        # retrieving json
        return self.app_request(self.token, method, app, dict_params=dict_params,
                                bl_retry_if_error=bl_retry_if_error,
                                float_secs_sleep=float_secs_sleep, bl_debug_mode=bl_debug_mode)

    def spxi_get(self, account_id, method='GET', app='/api/v1.0/account/{}/lmt/spxi'):
        '''
        DOCSTRING: GET SPXI INFORMATION FROM CLIENT CODE
        INPUTS: ACCOUNT CODE, METHOD, APP SPXI, KEY ID (FROM CLIENT INFOS)
        OUTPUTS: JSON - KEYS: INSTRUMENT ID, SYMBOL, SPCI, SPVI, SPCI EXCHANGE, SPVI EXCHANGE,
            IS REMOVED
        '''
        # parameters
        dict_params = {
            'accId': account_id,
        }
        #   retrieving json
        return self.app_request(self.token, method, app.format(account_id), dict_params)

    def spxi_instrument_post(self, account_id, dict_payload,
                             bl_parse_dict_params_data=True, method='POST',
                             app='/api/v1.0/account/{}/lmt/spxi'):
        '''
        DOCSTRING: SPXI INCLUSION TO ACCOUNT
        INPUTS: ACCOUNT ID, DICTIONARY PAYLOAD (LIST OF DICTIONARIES WITH KEYS: 'instrumentId': int,
            'isRemoved': 'false', 'spci': int, 'spvi': int, 'symbol': str), BOOLEAN PARSE DICT,
            METHOD (DEFAULT), APP SPXI (DEFAULT)
        OUTPUTS: STATUS CODE
        '''
        return self.app_request(self.token, method, app.format(account_id),
                                dict_payload=dict_payload,
                                bl_parse_dict_params_data=bl_parse_dict_params_data)

    def spxi_instrument_delete(self, account_id, dict_payload,
                               bl_parse_dict_params_data=True, method='POST',
                               app='/api/v1.0/account/{}/lmt/spxi'):
        '''
        DOCSTRING: SPXI INCLUSION TO DOCUMENT
        INPUTS: DOCUMENT ID, DICTIONARY PAYLOAD (LIST OF DICTIONARIES WITH KEYS: 'instrumentId': int,
            'isRemoved': 'true', 'symbol': str), BOOLEAN PARSE DICT, METHOD (DEFAULT), APP SPXI (DEFAULT)
        OUTPUTS: STATUS CODE
        '''
        return self.app_request(self.token, method, app.format(account_id),
                                dict_payload=dict_payload,
                                bl_parse_dict_params_data=bl_parse_dict_params_data)

    def spxi_tmox_global_metrics_remove(self, account_id, method='DELETE',
                                        app='/api/v1.0/account/{}/lmt'):
        '''
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        '''
        # parameters
        dict_params = {
            'accId': account_id,
        }
        # retrieving json
        return self.app_request(self.token, method, app.format(account_id), dict_params)

    def specific_global_metric_remotion(self, account_id, metric, method='DELETE',
                                        app='/api/v2.0/account/{}/lmt'):
        '''
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        '''
        # parameters
        dict_params = {
            'accId': account_id,
            'metric': metric
        }
        # retrieving json
        return self.app_request(self.token, method, app.format(account_id), dict_params)


class DocumentsData(ConnectionApi):

    def doc_info(self, doc_code, bl_retry_if_error=True, method='GET', app='/api/v1.0/document'):
        '''
        DOCSTRING: GET INFOS REGARDING DOCUMENT CODE (HIGLIGHT TO THE POSSIBILITY TO RETRIEVE 
            DOCUMENT ID FROM DOCUMENT CODE)
        INPUTS: DOCUMENT CODE, BL RETRY IF ERROR (DEFAULT), METHOD (DEFAULT), APP (DEFAULT)
        OUTPUTS: JSON
        '''
        # payload
        dict_params = {
            'participantCode': self.broker_code,
            'pnpCode': self.category_code,
            'documentCode': str(doc_code),
        }
        # retrieving json
        return self.app_request(self.token, method, app, dict_params=dict_params,
                                bl_retry_if_error=bl_retry_if_error)

    def block_unblock_doc(self, doc_id, bl_isblocked=True, bl_parse_dict_params_data=True, 
                          bl_retry_if_error=True,
                          method='POST', app='/api/v1.0/document/{}'):
        '''
        DOCSTRING: BLOCK DOCUMENT
        INPUTS: DOC_ID, BL_ISBLOCKED
        OUTPUTS: STATUS OF ACCOMPLISHMENT
        '''
        # payload
        dict_params = {
            'id': str(doc_id),
            'isBlocked': bl_isblocked
        }
        # retrieving json
        return self.app_request(self.token, method, app.format(str(doc_id)), 
                                dict_params=dict_params,
                                bl_parse_dict_params_data=bl_parse_dict_params_data,
                                bl_retry_if_error=bl_retry_if_error)

    def update_profile(self, doc_id, doc_profile_id, bl_parse_dict_params_data=True,
                       int_rmkt_evaluation=0, bl_retry_if_error=True, method='POST',
                       app='/api/v1.0/document/{}'):
        '''
        DOCSTRING: UPDATE DOCUMENT PROFILE
        INPUTS: DOC ID, DOC PROFILE ID, RMKT EVALUATION (DEFAULT)
        OUTPUTS: STATUS OF ACCOMPLISHMENT
        '''
        # payload
        dict_payload = {
            'id': str(doc_id),
            'profileFull': int(doc_profile_id),
            'rmktEvaluation': int_rmkt_evaluation
        }
        #   retrieving json
        return self.app_request(self.token, method, app.format(str(doc_id)),
                                dict_payload=dict_payload,
                                bl_parse_dict_params_data=bl_parse_dict_params_data,
                                bl_retry_if_error=bl_retry_if_error)

    def bl_protection_mode(self, doc_id, bl_protect=True, bl_parse_dict_params_data=True,
                           bl_retry_if_error=True, method='POST', app='/api/v1.0/document/{}'):
        '''
        DOCSTRING: PROTECTION MODE FOR THE CURRENT DOCUMET
        INPUTS: DOC ID, DOC PROFILE ID, RMKT EVALUATION (DEFAULT)
        OUTPUTS: STATUS OF ACCOMPLISHMENT
        '''
        # payload
        dict_payload = {
            'id': str(doc_id),
            'isProtected': str(bl_protect).lower(),
        }
        #   retrieving json
        return self.app_request(self.token, method, app.format(str(doc_id)), dict_payload=dict_payload,
                                bl_parse_dict_params_data=bl_parse_dict_params_data,
                                bl_retry_if_error=bl_retry_if_error)

    def client_infos(self, doc_id, bl_retry_if_error=True, float_secs_sleep=None, bl_debug_mode=False,
                     method='GET', app='/api/v1.0/account'):
        '''
        DOCSTRING: CLIENT REGISTER ON LINE B3
        INPUTS: DOC ID, METHOD (DEFAULT), APP CLIENT INFOS (DEFAULT)
        OUTPUTS: JSON - KEYS: INTERNAL ID, CODE, DOCUMENT, BOOLEAN PROTECTED, BOOLEAN BLOCKED,
            NAME, CATEGORY, SEGMENT, STATUS, PARTICIPANT NAME, PNP NAME, PARTICIPANT CODE,
            PNP CODE, PNP ACCOUNT CODE, PARTICIPANT ACCOUNT CODE, PARTICIPANT ACCOUNT CODE,
            ACCOUNT TYPE, OWNER DOCUMENT CODE, OWNER NAME
        '''
        # parameters
        dict_params = {
            'participantCode': self.broker_code,
            'pnpCode': self.category_code,
            'documentId': doc_id,
        }
        # retrieving json
        return self.app_request(self.token, method, app, dict_params=dict_params,
                                bl_retry_if_error=bl_retry_if_error,
                                float_secs_sleep=float_secs_sleep, bl_debug_mode=bl_debug_mode)

    def doc_profile(self, doc_id, method='GET', app='/api/v2.0/document/v2.0/document/{}',
                    key_api_line_b3_profile_full='profileFull',
                    key_api_line_b3_profile_name='profileName', bl_retry_if_error=True):
        '''
        DOCSTRING: DOC PROFILE (INTEGER AND NAME)
        INPUTS: DOC ID, METHOD (DEFAULT), APP CLIENT INFOS (DEFAULT)
        OUTPUTS: INTEGER AND STRIN
        '''
        #   fetch json
        json_doc = self.app_request(self.token, method, app.format(doc_id),
                                    bl_retry_if_error=bl_retry_if_error)
        #   returning profile info
        return {
            'profile_id': json_doc[key_api_line_b3_profile_full],
            'profile_name': json_doc[key_api_line_b3_profile_name],
        }

    def spxi_get(self, doc_id, method='GET', app='/api/v1.0/document/{}/lmt/spxi'):
        '''
        DOCSTRING: GET SPXI INFORMATION FROM CLIENT DOCUMENT
        INPUTS: DOC ID, METHOD, APP SPXI, KEY ID (FROM CLIENT INFOS)
        OUTPUTS: JSON - KEYS: INSTRUMENT ID, SYMBOL, SPCI, SPVI, SPCI EXCHANGE, SPVI EXCHANGE,
            IS REMOVED
        '''
        # parameters
        dict_params = {
            'docId': doc_id,
        }
        #   retrieving json
        return self.app_request(self.token, method, app.format(doc_id), dict_params)

    def spxi_instrument_post(self, doc_id, dict_payload,
                             bl_parse_dict_params_data=True, method='POST',
                             app='/api/v1.0/document/{}/lmt/spxi', bl_retry_if_error=True):
        '''
        DOCSTRING: SPXI INCLUSION TO DOCUMENT
        INPUTS: DOCUMENT ID, DICTIONARY PAYLOAD (LIST OF DICTIONARIES WITH KEYS: 'instrumentId': int,
            'isRemoved': 'false', 'spci': int, 'spvi': int, 'symbol': str), BOOLEAN PARSE DICT,
            METHOD (DEFAULT), APP SPXI (DEFAULT)
        OUTPUTS: STATUS CODE
        '''
        return self.app_request(self.token, method, app.format(doc_id),
                                dict_payload=dict_payload,
                                bl_parse_dict_params_data=bl_parse_dict_params_data,
                                bl_retry_if_error=bl_retry_if_error)

    def spxi_instrument_delete(self, doc_id, dict_payload,
                               bl_parse_dict_params_data=True, method='POST',
                               app='/api/v1.0/document/{}/lmt/spxi'):
        '''
        DOCSTRING: SPXI INCLUSION TO DOCUMENT
        INPUTS: DOCUMENT ID, DICTIONARY PAYLOAD (LIST OF DICTIONARIES WITH KEYS: 'instrumentId': int,
            'isRemoved': 'true', 'symbol': str), BOOLEAN PARSE DICT, METHOD (DEFAULT),
            APP SPXI (DEFAULT)
        OUTPUTS: STATUS CODE
        '''
        return self.app_request(self.token, method, app.format(doc_id),
                                dict_payload=dict_payload,
                                bl_parse_dict_params_data=bl_parse_dict_params_data)


class Professional(ConnectionApi):

    def professional_code_get(self, method='GET',
                              app='/api/v1.0/operationsProfessionalParticipant/code'):
        '''
        DOCSTRING: GET PROFESSIONAL INFORMATION FROM ITS CODE
        INPUTS: PROFESSIONAL CODE
        OUTPUTS: JSON WITH ID, NAME, BLOCKED/PROTECTED, PROFILE ID/NAME AND PROFESSIONAL CODE
        '''
        dict_params = {
            'participantCode': self.broker_code,
            'pnpCode': self.category_code
        }
        return self.app_request(self.token, method, app, dict_params)

    def professional_historic_position(self, professional_code, start_date, end_date,
                                       int_participant_perspective_type=0,
                                       list_metric_type=[
                                           1, 2, 3, 4, 6, 7, 22, 25, 26, 27, 28, 29, 36, 38, 39],
                                       entity_type=4, int_items_per_page=50, method='POST',
                                       app='https://api.line.trd.cert.bvmfnet.com.br/api/v2.0/position/hstry',
                                       bl_retry_if_error=True, bl_debug_mode=True,
                                       bl_parse_dict_params_data=True,
                                       float_secs_sleep=None):
        '''
        DOCSTRING: GET PROFESSIONAL POSITIONS HISTORIC FROM ITS CODE
        INPUTS: PROFESSIONAL CODE
        OUTPUTS: JSON WITH POSITIONS HISTORIC
        '''
        # payload for request
        dict_payload = {
            'angularItensPerPage': int_items_per_page,
            'entityType': entity_type,
            'metricTypes': list_metric_type,
            'ownerBrokerCode': int(self.broker_code),
            'ownerCategoryType': int(self.category_code),
            'partPerspecType': int_participant_perspective_type,
            'registryDateEnd': end_date,
            'registryDateStart': start_date,
            'traderCode': professional_code
        }
        if bl_debug_mode == True:
            pprint(dict_payload)
        # retrieving professional positions
        return self.app_request(self.token, method, app, dict_payload=dict_payload,
                                bl_retry_if_error=bl_retry_if_error, bl_debug_mode=bl_debug_mode,
                                bl_parse_dict_params_data=bl_parse_dict_params_data,
                                float_secs_sleep=float_secs_sleep)


class ProfilesData(ConnectionApi):

    @property
    def risk_profile(self, method='GET', app='/api/v1.0/riskProfile'):
        '''
        DOCSTRING: GET PROFILES AVAILABLE IN LINE B3
        INPUTS: PROFILE ID
        OUTPUTS: JSON WITH ID, NAME, BLOCKED/PROTECTED, PROFILE ID/NAME AND PROFESSIONAL CODE
        '''
        return self.app_request(self.token, method, app)

    def entities_associated_profile(self, id_profile, method='GET',
                                    app='/api/v1.0/riskProfile/enty', bl_retry_if_error=True):
        '''
        DOCSTRING: ENTITY DOCUMENTS LINKED TO THE PROFILE
        INPUTS: PROFILE ID
        OUTPUTS: JSON WITH ID, NAME, BLOCKED/PROTECTED, PROFILE ID/NAME AND PROFESSIONAL CODE
        '''
        dict_params = {
            'id': id_profile,
            'participantCode': self.broker_code,
            'pnpCode': self.category_code
        }
        return self.app_request(self.token, method, app, dict_params,
                                bl_retry_if_error=bl_retry_if_error)

    def profile_global_limits_get(self, prof_id, method='GET',
                                  app='/api/v1.0/riskProfile/{}/lmt'):
        '''
        DOCSTRING: GET PROFILE GLOBAL LIMITS BY ITS ID
        INPUTS: PROFILE ID
        OUTPUTS: JSON WITH ID, NAME, BLOCKED/PROTECTED, PROFILE ID/NAME AND PROFESSIONAL CODE
        '''
        return self.app_request(self.token, method, app.format(prof_id))

    def profile_market_limits_get(self, prof_id, method='GET',
                                  app='/api/v1.0/riskProfile/{}/lmt/mkta', bl_retry_if_error=True):
        '''
        DOCSTRING: GET PROFILE MARKET LIMITS BY ITS ID
        INPUTS: PROFILE ID
        OUTPUTS: JSON WITH ID, NAME, BLOCKED/PROTECTED, PROFILE ID/NAME AND PROFESSIONAL CODE
        '''
        return self.app_request(self.token, method, app.format(prof_id),

                                bl_retry_if_error=bl_retry_if_error)

    def profile_spxi_limits_get(self, prof_id, method='GET',
                                app='/api/v1.0/riskProfile/{}/lmt/spxi'):
        '''
        DOCSTRING: GET PROFILE SPCI/SPVI LIMITS BY ITS ID
        INPUTS: PROFILE ID
        OUTPUTS: JSON WITH ID, NAME, BLOCKED/PROTECTED, PROFILE ID/NAME AND PROFESSIONAL CODE
        '''
        return self.app_request(self.token, method, app.format(prof_id))

    def profile_tmox_limits_get(self, prof_id, method='GET',
                                app='/api/v1.0/riskProfile/{}/lmt/tmox'):
        '''
        DOCSTRING: GET PROFILE TMOC/TMOV LIMITS BY ITS ID
        INPUTS: PROFILE ID
        OUTPUTS: JSON WITH ID, NAME, BLOCKED/PROTECTED, PROFILE ID/NAME AND PROFESSIONAL CODE
        '''
        return self.app_request(self.token, method, app.format(prof_id))

    def spxi_instrument_post(self, prof_id, dict_payload,
                             bl_parse_dict_params_data=True, method='POST',
                             app='/api/v1.0/riskProfile/{}/lmt/tmox', bl_retry_if_error=True):
        '''
        DOCSTRING: SPXI INCLUSION TO DOCUMENT
        INPUTS: DOCUMENT ID, DICTIONARY PAYLOAD (LIST OF DICTIONARIES WITH KEYS: 'instrumentId': int,
            'isRemoved': false, 'symbol': 'RNEW4', 'tmoc': 0, 'tmocExchange': 0, 'tmov': 1000000,
            'tmovExchange': 0), BOOLEAN PARSE DICT, METHOD (DEFAULT), APP SPXI (DEFAULT)
        OUTPUTS: STATUS CODE
        '''
        return self.app_request(self.token, method, app.format(prof_id),
                                dict_payload=dict_payload,
                                bl_parse_dict_params_data=bl_parse_dict_params_data,
                                bl_retry_if_error=bl_retry_if_error)


class Monitoring(ConnectionApi):

    @property
    def alerts(self, method='GET', app='/api/v1.0/alert/lastalerts?filterRead=true',
               bl_retry_if_error=True):
        '''
        DOCSTRING: INSTRUMENTS INFORMATION
        INPUTS: MEHTOD (DEFAULT), APP INSTRUMENT INFORMATION (DEFAULT)
        OUTPUTS: JSON (ID, SYMBOL AND ASSET)
        '''
        return self.app_request(self.token, method, app, bl_retry_if_error=bl_retry_if_error)


class SystemEventManagement(ConnectionApi):

    def report(self, int_working_days_before=1, int_working_days_after=0,
               str_start_time='00:00', str_end_time='23:59', str_null='null',
               int_entity_type=3, method='POST',
               bl_parse_dict_params_data=True, float_secs_sleep=None,
               app='/api/v1.0/systemEvent'):
        '''
        DOCSTRING: SPXI INCLUSION TO DOCUMENT
        INPUTS: WORKING DAYS BEFORE AND AFTER
        OUTPUTS: STATUS CODE
        '''
        # payload to consult the range of dates of interest
        dict_payload = {
            'participantCode': int(self.broker_code),
            'categoryType': int(self.category_code),
            'entityType': int_entity_type,
            'carryingAccountCode': str_null,
            'pnpCode': '',
            'accountTypeLineDomain': str_null,
            'ownerName': str_null,
            'documentCode': str_null,
            'accountCode': str_null,
            'startTime': str_start_time,
            'endTime': str_end_time,
            'startDate': DatesBR().sub_working_days(DatesBR().curr_date(),
                                                    int_working_days_before).strftime('%d/%m/%Y'),
            'endDate': DatesBR().add_working_days(DatesBR().curr_date(),
                                                  int_working_days_after).strftime('%d/%m/%Y')
        }
        # return json
        return self.app_request(self.token, method, app, dict_payload=dict_payload,
                                bl_parse_dict_params_data=bl_parse_dict_params_data,
                                float_secs_sleep=float_secs_sleep)


# client_id = 's-3-7'
# client_secret = 'Ersg*80OP15'
# broker_code = '3'
# category_code = '3'
# id_profile = 194
# doc_id = 46817697
# id_ticker = 37005
# str_ticker = 'PETR4'
# str_asset = 'PETR'
# id_group_ibrx100 = 71
# doc_profile_id = 100268905
# codcli = 570921
# account_id = 29812111
# doc_code = '40965564835'
# list_codcli = [570921, 4967928]

# token = ConnectionApi(client_id, client_secret, broker_code, category_code).access_token
# print(token)

# print(SystemEventManagement(client_id, client_secret, broker_code, category_code, token).report())
# print(Monitoring(client_id, client_secret, broker_code, category_code, token).alerts)
# print(DocumentsData(client_id, client_secret, broker_code, category_code, token).doc_info(doc_code))
# print(DocumentsData(client_id, client_secret, broker_code, category_code,
#     token).client_infos(doc_id))
# print(DocumentsData(client_id, client_secret, broker_code, category_code,
#     token).update_profile(doc_id, doc_profile_id))
# print(DocumentsData(client_id, client_secret, broker_code, category_code,
#     token).bl_protection_mode(doc_id))
# opp_list = []
# for opp in Professional(client_id, client_secret, broker_code, category_code,
#     token).professional_code_get():
#     opp_list.append(opp['code'])
# for code in opp_list:
#     print(Professional(client_id, client_secret, broker_code, category_code,
#         token).professional_historic_position(code, start_date='18/04/2022', end_date='18/05/2022'))

# print(Professional(client_id, client_secret, broker_code, category_code,
#         token).professional_historic_position('JFW', start_date='05/04/2022', end_date='05/05/2022'))

# print(ProfilesData(client_id, client_secret, broker_code, category_code,
#     token).risk_profile)

# print(ProfilesData(client_id, client_secret, broker_code, category_code,
#     token).entities_associated_profile(id_profile))

# print(DocumentsData(client_id, client_secret, broker_code, category_code,
#     token).doc_profile(doc_id))

# print(ProfilesData(client_id, client_secret, broker_code, category_code,
#     token).profile_market_limits_get(id_profile))

# print(Operations(client_id, client_secret, broker_code, category_code,
#     token).intruments_per_group(id_group_ibrx100))

# print(Operations(client_id, client_secret, broker_code, category_code,
#     token).groups_authorized_markets)

# print(Operations(client_id, client_secret, broker_code, category_code,
#                  token).authorized_markets_instruments)

# print(AccountsData(client_id, client_secret, broker_code, category_code,
#                  token).client_infos(codcli, bl_debug_mode=True))

# print(DocumentsData(client_id, client_secret, broker_code, category_code,
#     token).spxi_instrument_post(doc_id, dict_payload=[{'instrumentId': 20597, 'isRemoved': 'true',
#     'symbol': 'VALE3'}]))

# print(AccountsData(client_id, client_secret, broker_code, category_code,
#                  token).clients_infos(list_codcli, bl_debug_mode=True))

# print(DocumentsData(client_id, client_secret, broker_code, category_code,
#     token).spxi_instrument_post(doc_id, dict_payload=[{'instrumentId': 30880,
#     'isRemoved': 'false', 'spci': 30000000, 'spvi': 30000000, 'symbol': 'LUPA3'}]))

# print(AccountsData(client_id, client_secret, broker_code, category_code,
#     token).spxi_instrument_post(account_id, dict_payload=[{'instrumentId': 30880,
#     'isRemoved': 'false', 'spci': 30000000, 'spvi': 30000000, 'symbol': 'LUPA3'}]))

# dict_payload = [{
#     "instrumentId": 1910,
#     "isRemoved": 'false',
#     "symbol": 'RNEW4',
#     "tmoc": 0,
#     "tmocExchange": 0,
#     "tmov": 15354354,
#     "tmovExchange": 0
# }]
# prof_id = 194
# print(ProfilesData(client_id, client_secret, broker_code, category_code, token).spxi_instrument_post(
#     prof_id, dict_payload))
