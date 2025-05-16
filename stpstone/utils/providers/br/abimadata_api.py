import pandas as pd
from typing import Any, Dict, List, Optional
from requests import exceptions, request
from stpstone._config.global_slots import YAML_ANBIMA_DATA_API
from stpstone.utils.cals.handling_dates import DatesBR
from stpstone.utils.parsers.dicts import HandlingDicts
from stpstone.utils.parsers.json import JsonFiles
from stpstone.utils.parsers.str import StrHandler


class AnbimaDataGen:

    def __init__(
        self,
        str_client_id: str,
        str_client_secret: str,
        str_env: str = "dev",
        bl_debug: bool = False,
        int_chunk: int = 1000,
        str_host_prd: str = "https://api.anbima.com.br/",
        str_host_dev: str = "https://api-sandbox.anbima.com.br/",
    ):
        """
        Anbima data API

        Args:
            str_client_id (str): string with client id
            str_client_secret (str): string with client secret
            str_env (str): string with environment
            bl_debug (bool): boolean with debug mode
            int_chunk (int): integer with chunk size
            str_host_prd (str): string with production host
            str_host_dev (str): string with development host

        Metadata: https://developers.anbima.com.br/api-portal/pt-br
        """
        self.str_client_id = str_client_id
        self.str_client_secret = str_client_secret
        self.bl_debug = bl_debug
        self.int_chunk = int_chunk
        self.str_host_prd = str_host_prd
        self.str_host = locals()[f"str_host_{str_env.lower()}"]
        self.str_token = self.access_token["access_token"]

    @property
    def access_token(
        self, str_app: str = "oauth/access-token", str_method: str = "POST"
    ) -> List[Dict[str, Any]]:
        str_url = self.str_host_prd + str_app
        base64_credentials = StrHandler().base64_encode(
            self.str_client_id, self.str_client_secret
        )
        dict_headers = {
            "Content-Type": "application/json",
            "Authorization": base64_credentials,
        }
        dict_payload = {"grant_type": "client_credentials"}
        resp_req = request(
            method=str_method,
            url=str_url,
            headers=dict_headers,
            data=JsonFiles().dict_to_json(dict_payload),
        )
        if self.bl_debug == True:
            print("TOKEN: {}".format(resp_req.json()["access_token"]))
        resp_req.raise_for_status()
        return resp_req.json()

    def generic_request(self, str_app: str, str_method: str) -> List[Dict[str, Any]]:
        str_url = self.str_host + str_app
        if self.bl_debug == True:
            print(f"URL: {str_url}")
        dict_headers = {
            "accept": "application/json",
            "client_id": self.str_client_id,
            "access_token": self.str_token,
        }
        resp_req = request(method=str_method, url=str_url, headers=dict_headers)
        resp_req.raise_for_status()
        return resp_req.json()


class AnbimaDataFunds(AnbimaDataGen):
    """
    Metadata: https://developers.anbima.com.br/pt/swagger-de-fundos-v2-rcvm-175/#/Notas%20explicativas/buscarNotasExplicativas
    """

    def funds_raw(
        self,
        int_pg: Optional[int] = None,
        str_app: str = "feed/fundos/v2/fundos?size={}&page={}",
        str_method: str = "GET",
    ) -> List[Dict[str, Any]]:
        """
        Retrieve available closed and opened end funds

        Args: -

        Returns:
            Json
        """
        return self.generic_request(
            str_app.format(
                self.int_chunk,
                int_pg,
            ),
            str_method,
        )

    def funds_trt(self, int_pg: int = 0) -> pd.DataFrame:
        """
        Treating available closed and opened end funds

        Args:
            int_pg (int): integer with page number

        Returns:
            pd.DataFrame
        """
        # setting variables
        list_ser = list()
        int_fnd = 0
        # looping within all available fund pages
        while True:
            #   requesting for current fund json, in case status code is different
            #       from 2xx, break the loop and return the dataframe
            try:
                json_funds = self.funds_raw(int_pg)
            except exceptions.HTTPError:
                break
            # looping within content dictionaries
            for dict_cnt in json_funds[YAML_ANBIMA_DATA_API["key_content"]]:
                #   setting variables
                dict_aux = dict()
                int_fnd += 1
                #   looping within keys and values from content
                for (
                    YAML_ANBIMA_DATA_API["key_cnt"],
                    data_cnt,
                ) in dict_cnt.items():
                    if isinstance(data_cnt, str):
                        dict_aux[YAML_ANBIMA_DATA_API["key_cnt"]] = (
                            data_cnt.strip()
                        )
                    elif data_cnt is None:
                        dict_aux[YAML_ANBIMA_DATA_API["key_cnt"]] = data_cnt
                    elif isinstance(data_cnt, list):
                        #   looping within classes
                        for i_cls, dict_cls in enumerate(data_cnt):
                            #   looping within classes and appending to serialized list
                            for (
                                YAML_ANBIMA_DATA_API["key_cls"],
                                data_cls,
                            ) in dict_cls.items():
                                if (
                                    YAML_ANBIMA_DATA_API["key_cls"]
                                    != YAML_ANBIMA_DATA_API["key_name_sbclss"]
                                ) and (data_cls is not None):
                                    dict_aux[
                                        YAML_ANBIMA_DATA_API["key_cls"]
                                    ] = data_cls.strip()
                                elif (
                                    YAML_ANBIMA_DATA_API["key_cls"]
                                    != YAML_ANBIMA_DATA_API["key_name_sbclss"]
                                ) and (data_cls is None):
                                    dict_aux[
                                        YAML_ANBIMA_DATA_API["key_cls"]
                                    ] = data_cls
                                elif (
                                    YAML_ANBIMA_DATA_API["key_cls"]
                                    == YAML_ANBIMA_DATA_API["key_name_sbclss"]
                                ) and (isinstance(data_cls, list)):
                                    #   looping within subclasses and copy auxiliary dicitionary in
                                    #       each iteration, in order to renew the subclass
                                    #       info imported
                                    for dict_sbcls in data_cls:
                                        dict_xpt = dict_aux.copy()
                                        for (
                                            YAML_ANBIMA_DATA_API["key_sbcls"],
                                            data_sbcls,
                                        ) in dict_sbcls.items():
                                            dict_xpt = HandlingDicts().merge_n_dicts(
                                                dict_xpt,
                                                {
                                                    "{}_{}".format(
                                                        YAML_ANBIMA_DATA_API["key_name_sbcls"],
                                                        YAML_ANBIMA_DATA_API["key_sbcls"],
                                                    ): data_sbcls
                                                },
                                                {
                                                    YAML_ANBIMA_DATA_API["col_num_fnd"]: int_fnd + 1,
                                                    YAML_ANBIMA_DATA_API["col_num_class"]: i_cls + 1,
                                                    YAML_ANBIMA_DATA_API["col_num_pg"]: int_pg,
                                                },
                                            )
                                        list_ser.append(dict_xpt)
                                elif (
                                    YAML_ANBIMA_DATA_API["key_cls"]
                                    == YAML_ANBIMA_DATA_API["key_name_sbclss"]
                                ) and (data_cls is None):
                                    list_ser.append(
                                        HandlingDicts().merge_n_dicts(
                                            dict_aux,
                                            {
                                                YAML_ANBIMA_DATA_API["key_cls"]: data_cls
                                            },
                                            {
                                                YAML_ANBIMA_DATA_API["col_num_fnd"]: int_fnd + 1,
                                                YAML_ANBIMA_DATA_API["col_num_class"]: i_cls + 1,
                                                YAML_ANBIMA_DATA_API["col_num_pg"]: int_pg,
                                            },
                                        )
                                    )
                                else:
                                    raise Exception(
                                        "Error of content within class, please revise "
                                        + "pg: {} / key: {} / data: {}".format(
                                            int_pg,
                                            YAML_ANBIMA_DATA_API["key_cls"],
                                            data_cls,
                                        )
                                    )
                    else:
                        raise Exception(
                            f"Error of content data type, please revise the data: {data_cnt}"
                        )
            #   adding iterator
            int_pg += 1
        # appending serialized list to pandas dataframe
        df_funds = pd.DataFrame(list_ser)
        # changing columns types
        for col_dt in [
            YAML_ANBIMA_DATA_API["col_fund_closure_dt"],
            YAML_ANBIMA_DATA_API["col_eff_dt"],
            YAML_ANBIMA_DATA_API["col_incpt_dt"],
            YAML_ANBIMA_DATA_API["col_closure_dt"],
            YAML_ANBIMA_DATA_API["col_sbc_incpt_dt"],
            YAML_ANBIMA_DATA_API["col_sbc_closure_dt"],
            YAML_ANBIMA_DATA_API["col_sbc_eff_dt"],
        ]:
            df_funds[col_dt].fillna(
                YAML_ANBIMA_DATA_API["str_dt_fill_na"], inplace=True
            )
            df_funds[col_dt] = [
                DatesBR().str_date_to_datetime(
                    d, YAML_ANBIMA_DATA_API["str_dt_format"]
                )
                for d in df_funds[col_dt]
            ]
        for col_dt in [
            YAML_ANBIMA_DATA_API["col_update_ts"],
            YAML_ANBIMA_DATA_API["col_sbc_update_dt"],
        ]:
            df_funds[col_dt].fillna(
                YAML_ANBIMA_DATA_API["str_ts_fill_na"], inplace=True
            )
            df_funds[col_dt] = [
                DatesBR().timestamp_to_date(
                    d, format=YAML_ANBIMA_DATA_API["str_dt_format"]
                )
                for d in df_funds[col_dt]
            ]
        df_funds.fillna(YAML_ANBIMA_DATA_API["str_fill_na"], inplace=True)
        df_funds = df_funds.astype(
            {
                YAML_ANBIMA_DATA_API["col_fund_code"]: str,
                YAML_ANBIMA_DATA_API["col_type_id"]: str,
                YAML_ANBIMA_DATA_API["col_fund_id"]: str,
                YAML_ANBIMA_DATA_API["col_comp_name"]: str,
                YAML_ANBIMA_DATA_API["col_trade_name"]: str,
                YAML_ANBIMA_DATA_API["col_fund_type"]: str,
                YAML_ANBIMA_DATA_API["col_class_code"]: str,
                YAML_ANBIMA_DATA_API["col_class_id_type"]: str,
                YAML_ANBIMA_DATA_API["col_class_id"]: str,
                YAML_ANBIMA_DATA_API["col_comp_class"]: str,
                YAML_ANBIMA_DATA_API["col_trd_class"]: str,
                YAML_ANBIMA_DATA_API["col_n1_ctg"]: str,
                YAML_ANBIMA_DATA_API["col_subclasses"]: str,
            }
        )
        # removing duplicates
        df_funds.drop_duplicates(inplace=True)
        # returning dataframe
        return df_funds

    def fund_raw(
        self,
        str_code_fnd: str,
        str_app: str = "feed/fundos/v2/fundos/{}/historico",
        str_method: str = "GET",
    ) -> List[Dict[str, Any]]:
        return self.generic_request(str_app.format(str_code_fnd), str_method)

    def fund_trt(self, list_code_fnds: list):
        # setting variables
        dict_dfs = dict()
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
                if isinstance(data_cnt, str) or data_cnt is None:
                    dict_aux[key_cnt] = data_cnt
                elif isinstance(data_cnt, list):
                    dict_xpt = dict_aux.copy()
                    for dict_data in data_cnt:
                        for key_data, data_data in dict_data.items():
                            #   checking wheter the data instance is string, or list
                            if isinstance(data_data, str) or data_data is None:
                                dict_xpt["{}_{}".format(key_cnt, key_data)] = data_data
                            elif isinstance(data_data, list):
                                for dict_hist in data_data:
                                    dict_xpt_2 = dict_xpt.copy()
                                    for key_hist, data_hist in dict_hist.items():
                                        dict_xpt_2[
                                            "{}_{}_{}".format(
                                                key_cnt, key_data, key_hist
                                            )
                                        ] = data_hist
                                    list_ser.append(dict_xpt_2)
                        #   regarding classes first-order dictionary has a key called
                        #       historico_classe, which is a list (in 2024-11-08), it is treated
                        #       separately in the code, in order to create the serialized list
                        if key_data != "classes":
                            list_ser.append(dict_xpt)
                    df_ = pd.DataFrame(list_ser)
                    #   changing data types within columns
                    for col_ in list(df_.columns):
                        #   date
                        if (
                            StrHandler().match_string_like(col_, "*data_*") == True
                        ) and (len(col_) == 10):
                            df_[col_].fillna(
                                YAML_ANBIMA_DATA_API["str_dt_fill_na"],
                                inplace=True,
                            )
                            df_[col_] = [
                                DatesBR().str_date_to_datetime(
                                    d, YAML_ANBIMA_DATA_API["str_dt_format"]
                                )
                                for d in df_[col_]
                            ]
                        #   timestamp
                        elif (
                            (StrHandler().match_string_like(col_, "*data_*") == True)
                            and (StrHandler().match_string_like(col_, "*T*") == True)
                            and (len(col_) > 10)
                        ):
                            df_[col_].fillna(
                                YAML_ANBIMA_DATA_API["str_ts_fill_na"],
                                inplace=True,
                            )
                            df_[col_] = [
                                DatesBR().timestamp_to_date(
                                    d,
                                    format=YAML_ANBIMA_DATA_API[
                                        "str_dt_format"
                                    ],
                                )
                                for d in df_[col_]
                            ]
                        #   float
                        elif (
                            StrHandler().match_string_like(col_, "*percentual_*")
                            == True
                        ):
                            df_[col_].fillna(
                                YAML_ANBIMA_DATA_API["str_float_fill_na"],
                                inplace=True,
                            )
                            df_[col_] = [float(x) for x in df_[col_]]
                        #   string
                        else:
                            df_[col_].fillna(
                                YAML_ANBIMA_DATA_API["str_fill_na"],
                                inplace=True,
                            )
                            df_[col_] = [str(x).strip() for x in df_[col_]]
                    #   appending to list of dataframe
                    dict_dfs[str_code_fnd].append(df_)
                else:
                    raise Exception(
                        "Type of data within the content of the fund {} ".format(str_code_fnd)
                        + "not found, please check the code. DATA_TYPE: {}".format(type(data_cnt))
                    )
        # returning list of dataframes with fund infos
        return dict_dfs

    def fund_hist(
        self,
        str_code_class: str,
        str_app: str = "feed/fundos/v2/fundos/{}/historico",
        str_method: str = "GET",
    ) -> List[Dict[str, Any]]:
        return self.generic_request(str_app.format(str_code_class), str_method)

    def segment_investor(
        self,
        str_code_class: str,
        str_app: str = "feed/fundos/v2/fundos/segmento-investidor/{}/patrimonio-liquido",
        str_method: str = "GET",
    ) -> List[Dict[str, Any]]:
        return self.generic_request(str_app.format(str_code_class), str_method)

    def time_series_fund(
        self,
        str_date_inf: str,
        str_date_sup: str,
        str_code_class: str,
        str_app: str = "feed/fundos/v2/fundos/{}/serie-historica",
        str_method: str = "GET",
    ) -> List[Dict[str, Any]]:
        dict_payload = {
            "size": self.int_chunk,
            "data-inicio": str_date_inf,
            "data-fim": str_date_sup,
        }
        return self.generic_request(
            str_app.format(str_code_class, self.int_chunk),
            str_method=str_method,
            dict_payload=dict_payload,
        )

    def funds_financials_dt(
        self,
        str_date_update: str,
        str_app: str = "feed/fundos/v2/fundos/serie-historica/lote",
        str_method: str = "GET",
    ) -> List[Dict[str, Any]]:
        dict_payload = {"data-atualizacao": str_date_update, "size": self.int_chunk}
        return self.generic_request(
            str_app, str_method=str_method, dict_payload=dict_payload
        )

    def funds_registration_data_dt(
        self,
        str_date_update: str,
        str_app: str = "feed/fundos/v2/fundos/dados-cadastrais/lote",
        str_method: str = "GET",
    ) -> List[Dict[str, Any]]:
        dict_payload = {"data-atualizacao": str_date_update, "size": self.int_chunk}
        return self.generic_request(
            str_app, str_method=str_method, dict_payload=dict_payload
        )

    @property
    def institutions(
        self,
        str_app: str = "feed/fundos/v2/fundos/instituicoes",
        str_method: str = "GET",
    ) -> List[Dict[str, Any]]:
        dict_payload = {"size": self.int_chunk}
        return self.generic_request(
            str_app, str_method=str_method, dict_payload=dict_payload
        )

    def institution(
        self,
        str_ein: str,
        str_app: str = "feed/fundos/v2/fundos/instituicoes/{}",
        str_method: str = "GET",
    ) -> List[Dict[str, Any]]:
        dict_payload = {"size": self.int_chunk}
        return self.generic_request(
            str_app.format(str_ein), str_method=str_method, dict_payload=dict_payload
        )

    def explanatory_notes_fund(
        self,
        str_code_class: str,
        str_app: str = "feed/fundos/v2/fundos/{}/notas-explicativas",
        str_method: str = "GET",
    ) -> List[Dict[str, Any]]:
        return self.generic_request(str_app.format(str_code_class), str_method)
