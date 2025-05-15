import time
import pandas as pd
from pprint import pprint
from typing import Any, Dict, List, Optional, Union
from requests import request
from stpstone.transformations.validation.metaclass_type_checker import TypeChecker
from stpstone.utils.cals.handling_dates import DatesBR
from stpstone.utils.parsers.json import JsonFiles


class ConnectionApi(metaclass=TypeChecker):
    """
    Metadata:
        - http://www.b3.com.br/data/files/2E/95/28/F1/EBD17610515A8076AC094EA8/GUIDE-TO-LINE-5.0-API.pdf,
        - https://line.bvmfnet.com.br/#/endpoints

    Notes: Please contact CAU manager account to request a service user
    """

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        broker_code: str,
        category_code: str,
        hostname_api_line_b3: str = "https://api.line.bvmfnet.com.br",
    ):
        self.client_id = client_id
        self.client_secret = client_secret
        self.broker_code = broker_code
        self.category_code = category_code
        self.hostname_api_line_b3 = hostname_api_line_b3
        self.token = self.access_token

    @property
    def auth_header(
        self,
        method: str = "GET",
        key_header: str = "header",
        int_max_retrieves: int = 1000,
        int_status_code_ok: int = 200,
        int_status_code_iteration: int = 400,
        bl_verify: bool = False,
        app: str = "/api/v1.0/token/authorization",
    ) -> str:
        # passing variables
        i = 0
        # requesting authorization authheader
        dict_headers = {
            "Accept": "application/json",
            "Content-Type": "application/x-www-form-urlencoded",
        }
        # looping while status code not a 2xx response
        while (int_status_code_iteration != int_status_code_ok) and (
            i <= int_max_retrieves
        ):
            try:
                resp_req = request(
                    method=method,
                    url=self.hostname_api_line_b3 + app,
                    headers=dict_headers,
                    verify=bl_verify,
                )
            except:
                continue
            int_status_code_iteration = resp_req.status_code
            i += 1
        # raises exception when not a 2xx response
        resp_req.raise_for_status()
        # getting authheader
        return resp_req.json()[key_header]

    @property
    def access_token(
        self,
        method: str = "POST",
        int_refresh_min_time: int = 4000,
        max_retrieves: int = 100,
        int_status_code_ok: int = 200,
        int_status_code_iteration: int = 400,
        key_refresh_token: str = "refresh_token",
        key_access_token: str = "access_token",
        key_expires_in: str = "expires_in",
        bl_str_dict_params: bool = False,
        bl_verify: bool = False,
        i_retrieves: int = 0,
        i_aux: int = 0,
        int_expiration_time: int = 0,
        app: str = "/api/oauth/token",
    ) -> str:
        # header
        dict_headers = {
            "Authorization": "Basic {}".format(self.auth_header),
        }
        # if expiration time is inferior to base time, trigger a refresh code
        while (int_expiration_time < int_refresh_min_time) and (
            i_retrieves < max_retrieves
        ):
            #   dict_params with grant type, username, password broker code and category code -
            #       if its the first retrieve use a different dict_params dictionary
            if i_retrieves == 0:
                dict_params = {
                    "grant_type": "password",
                    "username": str(self.client_id),
                    "password": str(self.client_secret),
                    "brokerCode": str(self.broker_code),
                    "categoryCode": str(self.category_code),
                }
            else:
                dict_params = {
                    "grant_type": "refresh_token",
                    "refresh_token": refresh_token,
                }
            #   coverting to string, if its user will
            if bl_str_dict_params == True:
                dict_params = "&".join(
                    "{}={}".format(k, v) for k, v in dict_params.items()
                )
            # looping while status code not a 2xx response
            while (int_status_code_iteration != int_status_code_ok) and (
                i_aux <= max_retrieves
            ):
                try:
                    resp_req = request(
                        method=method,
                        url=self.hostname_api_line_b3 + app,
                        headers=dict_headers,
                        params=dict_params,
                        verify=bl_verify,
                    )
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

    def app_request(
        self,
        method: str,
        app_line_b3: str,
        dict_params: Optional[Dict[str, Any]] = None,
        dict_payload: Optional[List[Dict[str, Any]]] = None,
        bl_parse_dict_params_data: bool = False,
        bl_retry_if_error: bool = False,
        bl_retry_request: bool = True,
        bl_debug_mode: bool = False,
        int_max_retrieves: int = 100,
        float_secs_sleep: Optional[float] = None,
        float_secs_sleep_increase_error: float = 1.0,
        int_status_code_ok: int = 200,
        list_int_http_error_token: List[int] = [401],
    ) -> Union[List[Dict[str, Any]], int]:
        # passing variables
        i = 0
        float_secs_sleep_iteration = float_secs_sleep
        # header
        dict_header = {
            "Authorization": "Bearer {}".format(self.token),
            "Content-Type": "application/json",
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
                    print("*** ATTEMPT REQUEST #{} ***".format(i))
                # # request
                # print('URL: {}'.format(self.hostname_api_line_b3 + app_line_b3))
                # print('PARAMS: {}'.format(dict_params))
                # print('DATA: {}'.format(dict_payload))
                try:
                    resp_req = request(
                        method=method,
                        url=self.hostname_api_line_b3 + app_line_b3,
                        headers=dict_header,
                        params=dict_params,
                        data=dict_payload,
                    )
                    # print('ENDPOINT + API: {}'.format(resp_req.url))
                    if resp_req.status_code == int_status_code_ok:
                        bl_retry_request = False
                    elif resp_req.status_code in list_int_http_error_token:
                        #   reset token wheter http error 401 has been reached
                        dict_header = {
                            "Authorization": "Bearer {}".format(self.access_token),
                            "Content-Type": "application/json",
                        }
                    else:
                        float_secs_sleep_iteration += float_secs_sleep_increase_error
                    if bl_debug_mode == True:
                        print("REQUEST SUCCESFULLY MADE")
                except:
                    bl_retry_request = True
                    if bl_debug_mode == True:
                        print("EXCEPTION IN REQUEST #{}".format(i))
                # wait
                if float_secs_sleep_iteration != None:
                    time.sleep(float_secs_sleep_iteration)
                # iteration increase
                i += 1
            # reseting variables
            float_secs_sleep_iteration = float_secs_sleep
        else:
            resp_req = request(
                method=method,
                url=self.hostname_api_line_b3 + app_line_b3,
                headers=dict_header,
                params=dict_params,
                data=dict_payload,
            )
            if bl_debug_mode == True:
                print("REQUEST SUCCESFULLY MADE")
        #   raises exception when not a 2xx response
        resp_req.raise_for_status()
        #   retrieving response
        try:
            return resp_req.json()
        except:
            return resp_req.status_code


class Operations(ConnectionApi):

    @property
    def exchange_limits(
        self,
        method: str = "GET",
        app: str = "/api/v1.0/exchangeLimits/spxi/{}",
        bl_retry_if_error: bool = True,
    ) -> Union[List[Dict[str, Any]], int]:
        return self.app_request(
            self.token,
            method,
            app.format(self.broker_code),
            bl_retry_if_error=bl_retry_if_error,
        )

    @property
    def groups_authorized_markets(
        self,
        method: str = "GET",
        app: str = "/api/v1.0/exchangeLimits/autorizedMarkets",
        bl_retry_if_error: bool = True,
    ) -> Union[List[Dict[str, Any]], int]:
        return self.app_request(
            self.token, method, app, bl_retry_if_error=bl_retry_if_error
        )

    def intruments_per_group(
        self,
        group_id: str,
        method: str = "POST",
        str_bl_settled: str = "true",
        bl_parse_dict_params_data: bool = True,
        float_secs_sleep: Optional[float] = None,
        app: str = "/api/v1.0/exchangeLimits/findInstruments",
    ) -> Union[List[Dict[str, Any]], int]:
        dict_payload = {
            "authorizedMarketGroupId": group_id,
            "isLimitSetted": str_bl_settled,
        }
        return self.app_request(
            self.token,
            method,
            app,
            dict_payload=dict_payload,
            bl_parse_dict_params_data=bl_parse_dict_params_data,
            float_secs_sleep=float_secs_sleep,
        )

    @property
    def authorized_markets_instruments(
        self,
        key_id: str = "id",
        key_name: str = "name",
        key_assets_associated: str = "assets_associated",
        key_limit_spci_opt: str = "limitSpciOption",
        key_limit_spvi_opt: str = "limitSpviOption",
        key_limit_spci: str = "limitSpci",
        key_limit_spvi: str = "limitSpvi",
        key_instrument_symbol: str = "instrumentSymbol",
        key_instrument_asset: str = "instrumentAsset",
    ) -> Dict[str, Union[str, int, float]]:
        # setting variables
        dict_export = dict()
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
                        dict_export[dict_[key_id]][key_assets_associated] = list()
                    else:
                        if key_limit_spci_opt in dict_assets:
                            dict_export[dict_[key_id]][key_assets_associated].append(
                                {
                                    "instrument_symbol": dict_assets[
                                        key_instrument_symbol
                                    ],
                                    "instrument_asset": dict_assets[
                                        key_instrument_asset
                                    ],
                                    "limit_spci": dict_assets[key_limit_spci],
                                    "limit_spvi": dict_assets[key_limit_spvi],
                                    "limit_spci_option": dict_assets[
                                        key_limit_spci_opt
                                    ],
                                    "limit_spvi_option": dict_assets[
                                        key_limit_spvi_opt
                                    ],
                                }
                            )
                        else:
                            dict_export[dict_[key_id]][key_assets_associated].append(
                                {
                                    "instrument_symbol": dict_assets[
                                        key_instrument_symbol
                                    ],
                                    "instrument_asset": dict_assets[
                                        key_instrument_asset
                                    ],
                                    "limit_spci": dict_assets[key_limit_spci],
                                    "limit_spvi": dict_assets[key_limit_spvi],
                                }
                            )
        #  return dictionary with ticker, id of authorized market, maximum spxi and maximum option
        #       spxi, in case it is available
        return dict_export


class Resources(Operations):

    @property
    def instrument_informations(
        self,
        method: str = "GET",
        app: str = "/api/v1.0/symbol",
        bl_retry_if_error: bool = True,
    ) -> Union[List[Dict[str, Any]], int]:
        return self.app_request(
            self.token, method, app, bl_retry_if_error=bl_retry_if_error
        )

    @property
    def instrument_infos_exchange_limits(
        self,
        key_infos_id: str = "id",
        key_exchange_limits_id: str = "instrumentId",
        key_symbol: str = "symbol",
    ) -> Dict[str, Dict[Union[str, int, float]]]:
        # dataframe of exchange limits
        df_exchange_limits = pd.DataFrame.from_dict(self.exchange_limits)
        # convert data types
        df_exchange_limits = df_exchange_limits.astype({key_exchange_limits_id: str})
        # dataframe of instrument informations
        df_instrument_informations = pd.DataFrame.from_dict(
            self.instrument_informations
        )
        # convert data types
        df_instrument_informations = df_instrument_informations.astype(
            {key_infos_id: str}
        )
        # left join instrument infos and exchange limits
        df_join_instruments = df_instrument_informations.merge(
            df_exchange_limits,
            how="left",
            left_on=key_infos_id,
            right_on=key_exchange_limits_id,
        )
        # rename columns of interest
        df_join_instruments = df_join_instruments.rename(
            columns={key_symbol + "_x": key_symbol}
        )
        # remove columns of interest
        df_join_instruments.drop(columns=[key_symbol + "_y"], inplace=True)
        # exporting dictionary with symbol as key
        return {
            row[key_symbol]: {col_: row[col_] for col_ in df_join_instruments.columns}
            for _, row in df_join_instruments.iterrows()
        }

    def instrument_id_by_symbol(
        self, symbol: str, method: str = "GET", app: str = "/api/v1.0/symbol/{}"
    ) -> Union[List[Dict[str, Any]], int]:
        return self.app_request(self.token, method, app.format(str(symbol)))


class AccountsData(ConnectionApi):

    def client_infos(
        self,
        account_code: str,
        bl_retry_if_error: bool = True,
        bl_debug_mode: bool = False,
        float_secs_sleep: Optional[float] = None,
        method: str = "GET",
        app: str = "/api/v1.0/account",
    ) -> Union[List[Dict[str, Any]], int]:
        # parameters
        dict_params = {
            "participantCode": self.broker_code,
            "pnpCode": self.category_code,
            "accountCode": account_code,
        }
        # retrieving json
        return self.app_request(
            self.token,
            method,
            app,
            dict_params=dict_params,
            bl_retry_if_error=bl_retry_if_error,
            float_secs_sleep=float_secs_sleep,
            bl_debug_mode=bl_debug_mode,
        )

    def spxi_get(
        self,
        account_id: str,
        method: str = "GET",
        app: str = "/api/v1.0/account/{}/lmt/spxi",
    ) -> Union[List[Dict[str, Any]], int]:
        # parameters
        dict_params = {
            "accId": account_id,
        }
        #   retrieving json
        return self.app_request(self.token, method, app.format(account_id), dict_params)

    def spxi_instrument_post(
        self,
        account_id: str,
        dict_payload: Union[List[Dict[str, Any]], None],
        bl_parse_dict_params_data: bool = True,
        method: str = "POST",
        app: str = "/api/v1.0/account/{}/lmt/spxi",
    ) -> Union[List[Dict[str, Any]], int]:
        """
        SPXI inclustion to account
        Args:
            account_id (str): account id
            dict_payload (dict):
                [{'instrumentId': int, 'isRemoved': 'false', 'spci': int, 'spvi': int, 'symbol': str}, ...]
            bl_parse_dict_params_data (bool): parse dictionary params data
            method (str): method
            app (str): app
        INPUTS: ACCOUNT ID, DICTIONARY PAYLOAD (LIST OF DICTIONARIES WITH KEYS: 'instrumentId': int,
            'isRemoved': 'false', 'spci': int, 'spvi': int, 'symbol': str), BOOLEAN PARSE DICT,
            METHOD (DEFAULT), APP SPXI (DEFAULT)
        OUTPUTS: STATUS CODE
        """
        return self.app_request(
            self.token,
            method,
            app.format(account_id),
            dict_payload=dict_payload,
            bl_parse_dict_params_data=bl_parse_dict_params_data,
        )

    def spxi_instrument_delete(
        self,
        account_id: str,
        dict_payload: Union[List[Dict[str, Any]], None],
        bl_parse_dict_params_data=True,
        method="POST",
        app="/api/v1.0/account/{}/lmt/spxi",
    ) -> Union[List[Dict[str, Any]], int]:
        """
        SPXI instrument removal
        Args:
            account_id (str): account id
            dict_payload (dict):
                [{'instrumentId': int, 'isRemoved': 'false', 'symbol': str}, ...]
            bl_parse_dict_params_data (bool): parse dictionary params data
            method (str): method
            app (str): app
        INPUTS: ACCOUNT ID, DICTIONARY PAYLOAD (LIST OF DICTIONARIES WITH KEYS: 'instrumentId': int,
            'isRemoved': 'false', 'spci': int, 'spvi': int, 'symbol': str), BOOLEAN PARSE DICT,
            METHOD (DEFAULT), APP SPXI (DEFAULT)
        OUTPUTS: STATUS CODE
        """
        return self.app_request(
            self.token,
            method,
            app.format(account_id),
            dict_payload=dict_payload,
            bl_parse_dict_params_data=bl_parse_dict_params_data,
        )

    def spxi_tmox_global_metrics_remove(
        self,
        account_id: str,
        method: str = "DELETE",
        app: str = "/api/v1.0/account/{}/lmt",
    ) -> Union[List[Dict[str, Any]], int]:
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        # parameters
        dict_params = {
            "accId": account_id,
        }
        # retrieving json
        return self.app_request(self.token, method, app.format(account_id), dict_params)

    def specific_global_metric_remotion(
        self,
        account_id: str,
        metric: str,
        method: str = "DELETE",
        app: str = "/api/v2.0/account/{}/lmt",
    ) -> Union[List[Dict[str, Any]], int]:
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        # parameters
        dict_params = {"accId": account_id, "metric": metric}
        # retrieving json
        return self.app_request(self.token, method, app.format(account_id), dict_params)


class DocumentsData(ConnectionApi):

    def doc_info(
        self,
        doc_code: str,
        bl_retry_if_error: bool = True,
        method: str = "GET",
        app: str = "/api/v1.0/document",
    ) -> Union[List[Dict[str, Any]], int]:
        """
        DOCSTRING: GET INFOS REGARDING DOCUMENT CODE (HIGLIGHT TO THE POSSIBILITY TO RETRIEVE
            DOCUMENT ID FROM DOCUMENT CODE)
        INPUTS: DOCUMENT CODE, BL RETRY IF ERROR (DEFAULT), METHOD (DEFAULT), APP (DEFAULT)
        OUTPUTS: JSON
        """
        # payload
        dict_params = {
            "participantCode": self.broker_code,
            "pnpCode": self.category_code,
            "documentCode": str(doc_code),
        }
        # retrieving json
        return self.app_request(
            self.token,
            method,
            app,
            dict_params=dict_params,
            bl_retry_if_error=bl_retry_if_error,
        )

    def block_unblock_doc(
        self,
        doc_id: str,
        bl_isblocked: bool = True,
        bl_parse_dict_params_data: bool = True,
        bl_retry_if_error: bool = True,
        method: str = "POST",
        app: str = "/api/v1.0/document/{}",
    ) -> Union[List[Dict[str, Any]], int]:
        """
        DOCSTRING: BLOCK DOCUMENT
        INPUTS: DOC_ID, BL_ISBLOCKED
        OUTPUTS: STATUS OF ACCOMPLISHMENT
        """
        # payload
        dict_params = {"id": str(doc_id), "isBlocked": bl_isblocked}
        # retrieving json
        return self.app_request(
            self.token,
            method,
            app.format(str(doc_id)),
            dict_params=dict_params,
            bl_parse_dict_params_data=bl_parse_dict_params_data,
            bl_retry_if_error=bl_retry_if_error,
        )

    def update_profile(
        self,
        doc_id: str,
        doc_profile_id: str,
        bl_parse_dict_params_data: bool = True,
        int_rmkt_evaluation: int = 0,
        bl_retry_if_error: bool = True,
        method: str = "POST",
        app: str = "/api/v1.0/document/{}",
    ) -> Union[List[Dict[str, Any]], int]:
        """
        DOCSTRING: UPDATE DOCUMENT PROFILE
        INPUTS: DOC ID, DOC PROFILE ID, RMKT EVALUATION (DEFAULT)
        OUTPUTS: STATUS OF ACCOMPLISHMENT
        """
        # payload
        dict_payload = {
            "id": str(doc_id),
            "profileFull": int(doc_profile_id),
            "rmktEvaluation": int_rmkt_evaluation,
        }
        #   retrieving json
        return self.app_request(
            self.token,
            method,
            app.format(str(doc_id)),
            dict_payload=dict_payload,
            bl_parse_dict_params_data=bl_parse_dict_params_data,
            bl_retry_if_error=bl_retry_if_error,
        )

    def bl_protection_mode(
        self,
        doc_id: str,
        bl_protect: bool = True,
        bl_parse_dict_params_data: bool = True,
        bl_retry_if_error: bool = True,
        method: str = "POST",
        app: str = "/api/v1.0/document/{}",
    ) -> Union[List[Dict[str, Any]], int]:
        """
        DOCSTRING: PROTECTION MODE FOR THE CURRENT DOCUMET
        INPUTS: DOC ID, DOC PROFILE ID, RMKT EVALUATION (DEFAULT)
        OUTPUTS: STATUS OF ACCOMPLISHMENT
        """
        # payload
        dict_payload = {
            "id": str(doc_id),
            "isProtected": str(bl_protect).lower(),
        }
        #   retrieving json
        return self.app_request(
            self.token,
            method,
            app.format(str(doc_id)),
            dict_payload=dict_payload,
            bl_parse_dict_params_data=bl_parse_dict_params_data,
            bl_retry_if_error=bl_retry_if_error,
        )

    def client_infos(
        self,
        doc_id: str,
        bl_retry_if_error: bool = True,
        float_secs_sleep: Optional[float] = None,
        bl_debug_mode: bool = False,
        method: str = "GET",
        app: str = "/api/v1.0/account",
    ) -> Union[List[Dict[str, Any]], int]:
        """
        DOCSTRING: CLIENT REGISTER ON LINE B3
        INPUTS: DOC ID, METHOD (DEFAULT), APP CLIENT INFOS (DEFAULT)
        OUTPUTS: JSON - KEYS: INTERNAL ID, CODE, DOCUMENT, BOOLEAN PROTECTED, BOOLEAN BLOCKED,
            NAME, CATEGORY, SEGMENT, STATUS, PARTICIPANT NAME, PNP NAME, PARTICIPANT CODE,
            PNP CODE, PNP ACCOUNT CODE, PARTICIPANT ACCOUNT CODE, PARTICIPANT ACCOUNT CODE,
            ACCOUNT TYPE, OWNER DOCUMENT CODE, OWNER NAME
        """
        # parameters
        dict_params = {
            "participantCode": self.broker_code,
            "pnpCode": self.category_code,
            "documentId": doc_id,
        }
        # retrieving json
        return self.app_request(
            self.token,
            method,
            app,
            dict_params=dict_params,
            bl_retry_if_error=bl_retry_if_error,
            float_secs_sleep=float_secs_sleep,
            bl_debug_mode=bl_debug_mode,
        )

    def doc_profile(
        self,
        doc_id: str,
        method: str = "GET",
        app: str = "/api/v2.0/document/v2.0/document/{}",
        key_api_line_b3_profile_full: str = "profileFull",
        key_api_line_b3_profile_name: str = "profileName",
        bl_retry_if_error: bool = True,
    ) -> Union[List[Dict[str, Any]], int]:
        """
        DOCSTRING: DOC PROFILE (INTEGER AND NAME)
        INPUTS: DOC ID, METHOD (DEFAULT), APP CLIENT INFOS (DEFAULT)
        OUTPUTS: INTEGER AND STRIN
        """
        #   fetch json
        json_doc = self.app_request(
            self.token, method, app.format(doc_id), bl_retry_if_error=bl_retry_if_error
        )
        #   returning profile info
        return {
            "profile_id": json_doc[key_api_line_b3_profile_full],
            "profile_name": json_doc[key_api_line_b3_profile_name],
        }

    def spxi_get(
        self,
        doc_id: str,
        method: str = "GET",
        app: str = "/api/v1.0/document/{}/lmt/spxi",
    ) -> Union[List[Dict[str, Any]], int]:
        """
        DOCSTRING: GET SPXI INFORMATION FROM CLIENT DOCUMENT
        INPUTS: DOC ID, METHOD, APP SPXI, KEY ID (FROM CLIENT INFOS)
        OUTPUTS: JSON - KEYS: INSTRUMENT ID, SYMBOL, SPCI, SPVI, SPCI EXCHANGE, SPVI EXCHANGE,
            IS REMOVED
        """
        # parameters
        dict_params = {
            "docId": doc_id,
        }
        #   retrieving json
        return self.app_request(self.token, method, app.format(doc_id), dict_params)

    def spxi_instrument_post(
        self,
        doc_id: str,
        dict_payload: Union[List[Dict[str, Any]], None],
        bl_parse_dict_params_data=True,
        method="POST",
        app="/api/v1.0/document/{}/lmt/spxi",
        bl_retry_if_error=True,
    ) -> Union[List[Dict[str, Any]], int]:
        """
        DOCSTRING: SPXI INCLUSION TO DOCUMENT
        INPUTS: DOCUMENT ID, DICTIONARY PAYLOAD (LIST OF DICTIONARIES WITH KEYS: 'instrumentId': int,
            'isRemoved': 'false', 'spci': int, 'spvi': int, 'symbol': str), BOOLEAN PARSE DICT,
            METHOD (DEFAULT), APP SPXI (DEFAULT)
        OUTPUTS: STATUS CODE
        """
        return self.app_request(
            self.token,
            method,
            app.format(doc_id),
            dict_payload=dict_payload,
            bl_parse_dict_params_data=bl_parse_dict_params_data,
            bl_retry_if_error=bl_retry_if_error,
        )

    def spxi_instrument_delete(
        self,
        doc_id: str,
        dict_payload: Union[List[Dict[str, Any]], None],
        bl_parse_dict_params_data: bool = True,
        method: str = "POST",
        app: str = "/api/v1.0/document/{}/lmt/spxi",
    ) -> Union[List[Dict[str, Any]], int]:
        """
        DOCSTRING: SPXI INCLUSION TO DOCUMENT
        INPUTS: DOCUMENT ID, DICTIONARY PAYLOAD (LIST OF DICTIONARIES WITH KEYS: 'instrumentId': int,
            'isRemoved': 'true', 'symbol': str), BOOLEAN PARSE DICT, METHOD (DEFAULT),
            APP SPXI (DEFAULT)
        OUTPUTS: STATUS CODE
        """
        return self.app_request(
            self.token,
            method,
            app.format(doc_id),
            dict_payload=dict_payload,
            bl_parse_dict_params_data=bl_parse_dict_params_data,
        )


class Professional(ConnectionApi):

    def professional_code_get(
        self,
        method: str = "GET",
        app: str = "/api/v1.0/operationsProfessionalParticipant/code",
    ) -> Union[List[Dict[str, Any]], int]:
        """
        DOCSTRING: GET PROFESSIONAL INFORMATION FROM ITS CODE
        INPUTS: PROFESSIONAL CODE
        OUTPUTS: JSON WITH ID, NAME, BLOCKED/PROTECTED, PROFILE ID/NAME AND PROFESSIONAL CODE
        """
        dict_params = {
            "participantCode": self.broker_code,
            "pnpCode": self.category_code,
        }
        return self.app_request(self.token, method, app, dict_params)

    def professional_historic_position(
        self,
        professional_code: str,
        dt_start: str,
        dt_end: str,
        int_participant_perspective_type: int = 0,
        list_metric_type: List[int] = [
            1,
            2,
            3,
            4,
            6,
            7,
            22,
            25,
            26,
            27,
            28,
            29,
            36,
            38,
            39,
        ],
        entity_type: int = 4,
        int_items_per_page: int = 50,
        method: str = "POST",
        app: str = "https://api.line.trd.cert.bvmfnet.com.br/api/v2.0/position/hstry",
        bl_retry_if_error: bool = True,
        bl_debug_mode: bool = True,
        bl_parse_dict_params_data: bool = True,
        float_secs_sleep: Optional[float] = None,
    ) -> Union[List[Dict[str, Any]], int]:
        """
        DOCSTRING: GET PROFESSIONAL POSITIONS HISTORIC FROM ITS CODE
        INPUTS: PROFESSIONAL CODE
        OUTPUTS: JSON WITH POSITIONS HISTORIC
        """
        # payload for request
        dict_payload = {
            "angularItensPerPage": int_items_per_page,
            "entityType": entity_type,
            "metricTypes": list_metric_type,
            "ownerBrokerCode": int(self.broker_code),
            "ownerCategoryType": int(self.category_code),
            "partPerspecType": int_participant_perspective_type,
            "registryDateEnd": dt_end,
            "registryDateStart": dt_start,
            "traderCode": professional_code,
        }
        if bl_debug_mode == True:
            pprint(dict_payload)
        # retrieving professional positions
        return self.app_request(
            self.token,
            method,
            app,
            dict_payload=dict_payload,
            bl_retry_if_error=bl_retry_if_error,
            bl_debug_mode=bl_debug_mode,
            bl_parse_dict_params_data=bl_parse_dict_params_data,
            float_secs_sleep=float_secs_sleep,
        )


class ProfilesData(ConnectionApi):

    @property
    def risk_profile(
        self, method: str = "GET", app: str = "/api/v1.0/riskProfile"
    ) -> Union[List[Dict[str, Any]], int]:
        """
        DOCSTRING: GET PROFILES AVAILABLE IN LINE B3
        INPUTS: PROFILE ID
        OUTPUTS: JSON WITH ID, NAME, BLOCKED/PROTECTED, PROFILE ID/NAME AND PROFESSIONAL CODE
        """
        return self.app_request(self.token, method, app)

    def entities_associated_profile(
        self,
        id_profile: str,
        method: str = "GET",
        app: str = "/api/v1.0/riskProfile/enty",
        bl_retry_if_error: bool = True,
    ) -> Union[List[Dict[str, Any]], int]:
        """
        DOCSTRING: ENTITY DOCUMENTS LINKED TO THE PROFILE
        INPUTS: PROFILE ID
        OUTPUTS: JSON WITH ID, NAME, BLOCKED/PROTECTED, PROFILE ID/NAME AND PROFESSIONAL CODE
        """
        dict_params = {
            "id": id_profile,
            "participantCode": self.broker_code,
            "pnpCode": self.category_code,
        }
        return self.app_request(
            self.token, method, app, dict_params, bl_retry_if_error=bl_retry_if_error
        )

    def profile_global_limits_get(
        self,
        prof_id: str,
        method: str = "GET",
        app: str = "/api/v1.0/riskProfile/{}/lmt",
    ) -> Union[List[Dict[str, Any]], int]:
        """
        DOCSTRING: GET PROFILE GLOBAL LIMITS BY ITS ID
        INPUTS: PROFILE ID
        OUTPUTS: JSON WITH ID, NAME, BLOCKED/PROTECTED, PROFILE ID/NAME AND PROFESSIONAL CODE
        """
        return self.app_request(self.token, method, app.format(prof_id))

    def profile_market_limits_get(
        self,
        prof_id: str,
        method: str = "GET",
        app: str = "/api/v1.0/riskProfile/{}/lmt/mkta",
        bl_retry_if_error: bool = True,
    ) -> Union[List[Dict[str, Any]], int]:
        """
        DOCSTRING: GET PROFILE MARKET LIMITS BY ITS ID
        INPUTS: PROFILE ID
        OUTPUTS: JSON WITH ID, NAME, BLOCKED/PROTECTED, PROFILE ID/NAME AND PROFESSIONAL CODE
        """
        return self.app_request(
            self.token, method, app.format(prof_id), bl_retry_if_error=bl_retry_if_error
        )

    def profile_spxi_limits_get(
        self,
        prof_id: str,
        method: str = "GET",
        app: str = "/api/v1.0/riskProfile/{}/lmt/spxi",
    ) -> Union[List[Dict[str, Any]], int]:
        """
        DOCSTRING: GET PROFILE SPCI/SPVI LIMITS BY ITS ID
        INPUTS: PROFILE ID
        OUTPUTS: JSON WITH ID, NAME, BLOCKED/PROTECTED, PROFILE ID/NAME AND PROFESSIONAL CODE
        """
        return self.app_request(self.token, method, app.format(prof_id))

    def profile_tmox_limits_get(
        self,
        prof_id: str,
        method: str = "GET",
        app: str = "/api/v1.0/riskProfile/{}/lmt/tmox",
    ) -> Union[List[Dict[str, Any]], int]:
        """
        DOCSTRING: GET PROFILE TMOC/TMOV LIMITS BY ITS ID
        INPUTS: PROFILE ID
        OUTPUTS: JSON WITH ID, NAME, BLOCKED/PROTECTED, PROFILE ID/NAME AND PROFESSIONAL CODE
        """
        return self.app_request(self.token, method, app.format(prof_id))

    def spxi_instrument_post(
        self,
        prof_id: str,
        dict_payload: List[Dict[str, Any]],
        bl_parse_dict_params_data: bool = True,
        method: str = "POST",
        app: str = "/api/v1.0/riskProfile/{}/lmt/tmox",
        bl_retry_if_error: bool = True,
    ) -> Union[List[Dict[str, Any]], int]:
        """
        DOCSTRING: SPXI INCLUSION TO DOCUMENT
        INPUTS: DOCUMENT ID, DICTIONARY PAYLOAD (LIST OF DICTIONARIES WITH KEYS: 'instrumentId': int,
            'isRemoved': false, 'symbol': 'RNEW4', 'tmoc': 0, 'tmocExchange': 0, 'tmov': 1000000,
            'tmovExchange': 0), BOOLEAN PARSE DICT, METHOD (DEFAULT), APP SPXI (DEFAULT)
        OUTPUTS: STATUS CODE
        """
        return self.app_request(
            self.token,
            method,
            app.format(prof_id),
            dict_payload=dict_payload,
            bl_parse_dict_params_data=bl_parse_dict_params_data,
            bl_retry_if_error=bl_retry_if_error,
        )


class Monitoring(ConnectionApi):

    @property
    def alerts(
        self,
        method: str = "GET",
        app: str = "/api/v1.0/alert/lastalerts?filterRead=true",
        bl_retry_if_error: bool = True,
    ) -> Union[List[Dict[str, Any]], int]:
        """
        DOCSTRING: INSTRUMENTS INFORMATION
        INPUTS: MEHTOD (DEFAULT), APP INSTRUMENT INFORMATION (DEFAULT)
        OUTPUTS: JSON (ID, SYMBOL AND ASSET)
        """
        return self.app_request(
            self.token, method, app, bl_retry_if_error=bl_retry_if_error
        )


class SystemEventManagement(ConnectionApi):

    def report(
        self,
        int_working_days_before: int = 1,
        int_working_days_after: int = 0,
        str_start_time: str = "00:00",
        str_sup_time: str = "23:59",
        str_null: str = "null",
        int_entity_type: int = 3,
        method: str = "POST",
        bl_parse_dict_params_data: str = True,
        float_secs_sleep: Optional[float] = None,
        app: str = "/api/v1.0/systemEvent",
    ) -> Union[List[Dict[str, Any]], int]:
        """
        DOCSTRING: SPXI INCLUSION TO DOCUMENT
        INPUTS: WORKING DAYS BEFORE AND AFTER
        OUTPUTS: STATUS CODE
        """
        # payload to consult the range of dates of interest
        dict_payload = {
            "participantCode": int(self.broker_code),
            "categoryType": int(self.category_code),
            "entityType": int_entity_type,
            "carryingAccountCode": str_null,
            "pnpCode": "",
            "accountTypeLineDomain": str_null,
            "ownerName": str_null,
            "documentCode": str_null,
            "accountCode": str_null,
            "startTime": str_start_time,
            "endTime": str_sup_time,
            "startDate": DatesBR()
            .sub_working_days(DatesBR().curr_date(), int_working_days_before)
            .strftime("%d/%m/%Y"),
            "endDate": DatesBR()
            .add_working_days(DatesBR().curr_date(), int_working_days_after)
            .strftime("%d/%m/%Y"),
        }
        # return json
        return self.app_request(
            self.token,
            method,
            app,
            dict_payload=dict_payload,
            bl_parse_dict_params_data=bl_parse_dict_params_data,
            float_secs_sleep=float_secs_sleep,
        )
