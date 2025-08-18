"""ANBIMA Data API client implementation.

This module provides classes for interacting with ANBIMA's fund data API, including
authentication, data retrieval and processing functionalities. It handles both raw
API responses and processed DataFrame outputs.

References
----------
.. [1] https://developers.anbima.com.br/pt/
.. [2] https://developers.anbima.com.br/pt/swagger-de-fundos-v2-rcvm-175/#/Notas%20explicativas/buscarNotasExplicativas
"""

from typing import Any, Literal, Optional, TypedDict

import pandas as pd
from requests import exceptions, request

from stpstone._config.global_slots import YAML_ANBIMA_DATA_API
from stpstone.transformations.validation.metaclass_type_checker import TypeChecker
from stpstone.utils.cals.handling_dates import DatesBR
from stpstone.utils.parsers.dicts import HandlingDicts
from stpstone.utils.parsers.json import JsonFiles
from stpstone.utils.parsers.lists import ListHandler
from stpstone.utils.parsers.str import StrHandler


class ReturnAccessToken(TypedDict, metaclass=TypeChecker):
    """Typed dictionary for access token response.

    Parameters
    ----------
    access_token : str
        Authentication token string
    """

    access_token: str


class AnbimaDataGen(metaclass=TypeChecker):
    """Base class for ANBIMA API interactions."""

    def __init__(
        self,
        str_client_id: str,
        str_client_secret: str,
        str_env: Literal["dev", "prd"] = "dev",
        int_chunk: int = 1000,
    ) -> None:
        """Initialize ANBIMA API client.

        Parameters
        ----------
        str_client_id : str
            Client ID for API authentication
        str_client_secret : str
            Client secret for API authentication
        str_env : Literal['dev', 'prd']
            Environment ('dev' or 'prd'), defaults to 'dev'
        int_chunk : int
            Chunk size for paginated requests, defaults to 1000

        Returns
        -------
        None

        Raises
        ------
        ValueError
            If str_client_id or str_client_secret is empty
        """
        if not str_client_id or len(str_client_id.strip()) == 0:
            raise ValueError("str_client_id cannot be empty")
        if not str_client_secret or len(str_client_secret.strip()) == 0:
            raise ValueError("str_client_secret cannot be empty")
        self.str_client_id = str_client_id
        self.str_client_secret = str_client_secret
        self.int_chunk = int_chunk
        self.str_host = "https://api-sandbox.anbima.com.br/" if str_env == "dev" \
            else "https://api.anbima.com.br/"
        self._token_cache = None
        self.str_token = self.access_token()["access_token"]

    def access_token(self) -> ReturnAccessToken:
        """Retrieve access token for API authentication.

        Returns
        -------
        ReturnAccessToken
            Dictionary containing access token

        Raises
        ------
        ValueError
            If authentication fails
        """
        if self._token_cache:
            return self._token_cache
        str_url = "https://api.anbima.com.br/oauth/access-token"
        base64_credentials = StrHandler().base64_encode(self.str_client_id, self.str_client_secret)
        dict_headers = {"Content-Type": "application/json", "Authorization": base64_credentials}
        dict_payload = {"grant_type": "client_credentials"}
        try:
            resp_req = request(
                method="POST", 
                url=str_url, 
                headers=dict_headers, 
                data=JsonFiles().dict_to_json(dict_payload), 
                timeout=(200, 200)
            )
            resp_req.raise_for_status()
            self._token_cache = resp_req.json()
            return self._token_cache
        except Exception as err:
            raise ValueError("Failed to retrieve access token") from err

    def generic_request(
        self, 
        str_app: str, 
        str_method: Literal["GET", "POST"],
        dict_payload: Optional[dict] = None
    ) -> list[dict[str, Any]]:
        """Make generic API request.

        Parameters
        ----------
        str_app : str
            API endpoint path
        str_method : Literal['GET', 'POST']
            HTTP method
        dict_payload : Optional[dict], optional
            Request payload, defaults to None

        Returns
        -------
        list[dict[str, Any]]
            JSON response as list of dictionaries

        Raises
        ------
        ValueError
            If request fails
        """
        str_url = self.str_host + str_app
        dict_headers = {
            "accept": "application/json",
            "client_id": self.str_client_id,
            "access_token": self.str_token,
        }
        try:
            params = dict_payload if str_method == "GET" else None
            data = JsonFiles().dict_to_json(dict_payload) if str_method == "POST" \
                and dict_payload else None
            resp_req = request(
                method=str_method, 
                url=str_url, 
                headers=dict_headers, 
                params=params,
                data=data,
                timeout=(200, 200)
            )
            resp_req.raise_for_status()
            return resp_req.json()
        except Exception as err:
            raise ValueError(f"API request failed: {str(err)}") from err


class AnbimaDataFunds(AnbimaDataGen):
    """Class for interacting with ANBIMA funds data API."""

    def funds_raw(self, int_pg: Optional[int] = None) -> list[dict[str, Any]]:
        """Retrieve raw funds data from API.

        Parameters
        ----------
        int_pg : Optional[int]
            Page number, defaults to None

        Returns
        -------
        list[dict[str, Any]]
            List of fund data dictionaries
        """
        return self.generic_request(
            f"feed/fundos/v2/fundos?size={self.int_chunk}&page={int_pg}", "GET")

    def funds_trt(self, int_pg: int = 0) -> pd.DataFrame:
        """Process and transform raw funds data into DataFrame.

        Parameters
        ----------
        int_pg : int
            Starting page number, defaults to 0

        Returns
        -------
        pd.DataFrame
            Processed funds data as DataFrame

        Raises
        ------
        ValueError
            Invalid content in class
            Invalid content data type
        """
        list_ser = []
        int_fnd = 0

        while True:
            try:
                json_funds = self.funds_raw(int_pg)
                if not isinstance(json_funds, list) or not json_funds:
                    raise ValueError("Invalid funds data format")
            except exceptions.HTTPError:
                break

            for dict_cnt in json_funds[YAML_ANBIMA_DATA_API["key_content"]]:
                dict_aux = {}
                int_fnd += 1

                for key_cnt, data_cnt in dict_cnt.items():
                    if isinstance(data_cnt, str):
                        dict_aux[key_cnt] = data_cnt.strip()
                    elif data_cnt is None:
                        dict_aux[key_cnt] = data_cnt
                    elif isinstance(data_cnt, list):
                        for i_cls, dict_cls in enumerate(data_cnt):
                            for key_cls, data_cls in dict_cls.items():
                                if (
                                    key_cls != YAML_ANBIMA_DATA_API["key_name_sbclss"]
                                    and data_cls is not None
                                ):
                                    dict_aux[key_cls] = data_cls.strip()
                                elif (
                                    key_cls != YAML_ANBIMA_DATA_API["key_name_sbclss"]
                                    and data_cls is None
                                ):
                                    dict_aux[key_cls] = data_cls
                                elif (
                                    key_cls == YAML_ANBIMA_DATA_API["key_name_sbclss"]
                                    and isinstance(data_cls, list)
                                ):
                                    for dict_sbcls in data_cls:
                                        dict_xpt = dict_aux.copy()
                                        for key_sbcls, data_sbcls in dict_sbcls.items():
                                            dict_xpt = HandlingDicts().merge_n_dicts(
                                                dict_xpt,
                                                {
                                                    "{}_{}".format(
                                                        YAML_ANBIMA_DATA_API['key_name_sbcls'], 
                                                        key_sbcls
                                                    ): data_sbcls
                                                },
                                                {
                                                    YAML_ANBIMA_DATA_API["col_num_fnd"]: \
                                                        int_fnd + 1,
                                                    YAML_ANBIMA_DATA_API["col_num_class"]: \
                                                        i_cls + 1,
                                                    YAML_ANBIMA_DATA_API["col_num_pg"]: int_pg,
                                                },
                                            )
                                        list_ser.append(dict_xpt)
                                elif (
                                    key_cls == YAML_ANBIMA_DATA_API["key_name_sbclss"]
                                    and data_cls is None
                                ):
                                    list_ser.append(
                                        HandlingDicts().merge_n_dicts(
                                            dict_aux,
                                            {key_cls: data_cls},
                                            {
                                                YAML_ANBIMA_DATA_API["col_num_fnd"]: int_fnd + 1,
                                                YAML_ANBIMA_DATA_API["col_num_class"]: i_cls + 1,
                                                YAML_ANBIMA_DATA_API["col_num_pg"]: int_pg,
                                            },
                                        )
                                    )
                                else:
                                    raise ValueError(
                                        f"Invalid content in class: page={int_pg}, "
                                        f"key={key_cls}, data={data_cls}"
                                    )
                    else:
                        raise ValueError(f"Invalid content data type: {data_cnt}")

            int_pg += 1

        df_funds = pd.DataFrame(list_ser)
        df_funds = self._process_funds_dataframe(df_funds)
        return df_funds

    def _process_funds_dataframe(self, df_funds: pd.DataFrame) -> pd.DataFrame:
        """Process and clean funds DataFrame in-place.

        Parameters
        ----------
        df_funds : pd.DataFrame
            DataFrame to process

        Returns
        -------
        pd.DataFrame
            Processed funds DataFrame

        Raises
        ------
        ValueError
            If the DataFrame is missing expected columns
        """
        list_date_cols = [
            YAML_ANBIMA_DATA_API["col_fund_closure_dt"],
            YAML_ANBIMA_DATA_API["col_eff_dt"],
            YAML_ANBIMA_DATA_API["col_incpt_dt"],
            YAML_ANBIMA_DATA_API["col_closure_dt"],
            YAML_ANBIMA_DATA_API["col_sbc_incpt_dt"],
            YAML_ANBIMA_DATA_API["col_sbc_closure_dt"],
            YAML_ANBIMA_DATA_API["col_sbc_eff_dt"],
        ]
        list_ts_cols = [
            YAML_ANBIMA_DATA_API["col_update_ts"],
            YAML_ANBIMA_DATA_API["col_sbc_update_dt"],
        ]
        list_str_cols = [
            YAML_ANBIMA_DATA_API["col_fund_code"],
            YAML_ANBIMA_DATA_API["col_type_id"],
            YAML_ANBIMA_DATA_API["col_fund_id"],
            YAML_ANBIMA_DATA_API["col_comp_name"],
            YAML_ANBIMA_DATA_API["col_trade_name"],
            YAML_ANBIMA_DATA_API["col_fund_type"],
            YAML_ANBIMA_DATA_API["col_class_code"],
            YAML_ANBIMA_DATA_API["col_class_id_type"],
            YAML_ANBIMA_DATA_API["col_class_id"],
            YAML_ANBIMA_DATA_API["col_comp_class"],
            YAML_ANBIMA_DATA_API["col_trd_class"],
            YAML_ANBIMA_DATA_API["col_n1_ctg"],
            YAML_ANBIMA_DATA_API["col_subclasses"],
        ]
        if any(col not in df_funds.columns for col in 
               ListHandler().extend_lists(list_date_cols, list_ts_cols)):
            raise ValueError("Missing expected columns in DataFrame")

        for col in list_date_cols:
            df_funds = df_funds[col].fillna(YAML_ANBIMA_DATA_API["str_dt_fill_na"])
            df_funds[col] = [
                DatesBR().str_date_to_datetime(d, YAML_ANBIMA_DATA_API["str_dt_format"])
                for d in df_funds[col]
            ]

        for col in list_ts_cols:
            df_funds = df_funds[col].fillna(YAML_ANBIMA_DATA_API["str_ts_fill_na"])
            df_funds[col] = [
                DatesBR().timestamp_to_date(d, format=YAML_ANBIMA_DATA_API["str_dt_format"])
                for d in df_funds[col]
            ]

        for col in list_str_cols:
            if col in df_funds.columns:
                df_funds[col] = df_funds[col].fillna(YAML_ANBIMA_DATA_API["str_fill_na"])
                df_funds[col] = df_funds[col].astype(str).str.strip()

        df_funds = df_funds.drop_duplicates()
        return df_funds

    def fund_raw(self, str_code_fnd: str) -> list[dict[str, Any]]:
        """Retrieve raw fund historical data.

        Parameters
        ----------
        str_code_fnd : str
            Fund code identifier

        Returns
        -------
        list[dict[str, Any]]
            List of historical data dictionaries
        """
        return self.generic_request(f"feed/fundos/v2/fundos/{str_code_fnd}/historico", "GET")

    def fund_trt(self, list_code_fnds: list[str]) -> dict[str, list[pd.DataFrame]]:
        """Process multiple funds' historical data.

        Parameters
        ----------
        list_code_fnds : list[str]
            List of fund codes to process

        Returns
        -------
        dict[str, list[pd.DataFrame]]
            Dictionary mapping fund codes to their processed DataFrames

        Raises
        ------
        ValueError
            If an invalid fund code is provided
        """
        dict_dfs = {}
        for str_code_fnd in list_code_fnds:
            dict_aux = {}
            list_ser = []
            dict_dfs[str_code_fnd] = []
            json_fnd_info = self.fund_raw(str_code_fnd)

            if not isinstance(json_fnd_info, dict):
                raise ValueError(
                    f"Invalid data type for fund {str_code_fnd}: {type(json_fnd_info)}")

            for key_cnt, data_cnt in json_fnd_info.items():
                if isinstance(data_cnt, (str, type(None))):
                    dict_aux[key_cnt] = data_cnt
                elif isinstance(data_cnt, list):
                    dict_xpt = dict_aux.copy()
                    for dict_data in data_cnt:
                        for key_data, data_data in dict_data.items():
                            if isinstance(data_data, (str, type(None))):
                                dict_xpt[f"{key_cnt}_{key_data}"] = data_data
                            elif isinstance(data_data, list):
                                for dict_hist in data_data:
                                    dict_xpt_2 = dict_xpt.copy()
                                    for key_hist, data_hist in dict_hist.items():
                                        dict_xpt_2[
                                            f"{key_cnt}_{key_data}_{key_hist}"
                                        ] = data_hist
                                    list_ser.append(dict_xpt_2)
                        if key_data != "classes":
                            list_ser.append(dict_xpt)

                    df_ = pd.DataFrame(list_ser)
                    df_ = self._process_fund_dataframe(df_)
                    dict_dfs[str_code_fnd].append(df_)
                else:
                    raise ValueError(
                        f"Invalid data type for fund {str_code_fnd}: {type(data_cnt)}")
                df_ = pd.DataFrame(list_ser)
                df_ = self._process_fund_dataframe(df_)
                dict_dfs[str_code_fnd].append(df_)

            return dict_dfs

    def _process_fund_dataframe(self, df_: pd.DataFrame) -> pd.DataFrame:
        """Process and clean fund DataFrame in-place.

        Parameters
        ----------
        df_ : pd.DataFrame
            DataFrame to process

        Returns
        -------
        pd.DataFrame
            Processed DataFrame

        Raises
        ------
        ValueError
            If the DataFrame is missing expected columns
        """
        list_expected_columns = [
            "classes_historical_data_date",
            "classes_historical_data_timestamp",
            "classes_historical_data_percentual_value",
            "fund_code",
        ]
        # check for missing columns
        list_missing_cols = [col for col in list_expected_columns if col not in df_.columns]
        if list_missing_cols:
            raise ValueError(f"Missing expected columns in DataFrame: {list_missing_cols}")
        
        for col in df_.columns:
            if StrHandler().match_string_like(col, "*data_*") and len(col) == 10:
                df_ = df_[col].fillna(YAML_ANBIMA_DATA_API["str_dt_fill_na"])
                df_[col] = [
                    DatesBR().str_date_to_datetime(
                        d, YAML_ANBIMA_DATA_API["str_dt_format"]
                    )
                    for d in df_[col]
                ]
            elif (
                StrHandler().match_string_like(col, "*data_*")
                and StrHandler().match_string_like(col, "*T*")
                and len(col) > 10
            ):
                df_ = df_[col].fillna(YAML_ANBIMA_DATA_API["str_ts_fill_na"])
                df_[col] = [
                    DatesBR().timestamp_to_date(
                        d, format=YAML_ANBIMA_DATA_API["str_dt_format"]
                    )
                    for d in df_[col]
                ]
            elif StrHandler().match_string_like(col, "*percentual_*"):
                df_ = df_[col].fillna(YAML_ANBIMA_DATA_API["str_float_fill_na"])
                df_[col] = [float(x) for x in df_[col]]
            else:
                df_ = df_[col].fillna(YAML_ANBIMA_DATA_API["str_fill_na"])
                df_[col] = [str(x).strip() for x in df_[col]]

        return df_

    def fund_hist(self, str_code_class: str) -> list[dict[str, Any]]:
        """Retrieve fund historical data.

        Parameters
        ----------
        str_code_class : str
            Fund class code

        Returns
        -------
        list[dict[str, Any]]
            List of historical data dictionaries
        """
        return self.generic_request(f"feed/fundos/v2/fundos/{str_code_class}/historico", "GET")

    def segment_investor(self, str_code_class: str) -> list[dict[str, Any]]:
        """Retrieve investor segment data.

        Parameters
        ----------
        str_code_class : str
            Fund class code

        Returns
        -------
        list[dict[str, Any]]
            List of investor segment dictionaries
        """
        return self.generic_request(
            f"feed/fundos/v2/fundos/segmento-investidor/{str_code_class}/patrimonio-liquido", 
            "GET"
        )

    def time_series_fund(
        self,
        str_date_inf: str,
        str_date_sup: str,
        str_code_class: str,
    ) -> list[dict[str, Any]]:
        """Retrieve fund time series data.

        Parameters
        ----------
        str_date_inf : str
            Start date in YYYY-MM-DD format
        str_date_sup : str
            End date in YYYY-MM-DD format
        str_code_class : str
            Fund class code

        Returns
        -------
        list[dict[str, Any]]
            List of time series dictionaries
        """
        dict_payload = {
            "size": self.int_chunk,
            "data-inicio": str_date_inf,
            "data-fim": str_date_sup,
        }
        return self.generic_request(
            f"feed/fundos/v2/fundos/{str_code_class}/serie-historica",
            str_method="GET",
            dict_payload=dict_payload,
        )

    def funds_financials_dt(self, str_date_update: str) -> list[dict[str, Any]]:
        """Retrieve financials data by update date.

        Parameters
        ----------
        str_date_update : str
            Update date in YYYY-MM-DD format

        Returns
        -------
        list[dict[str, Any]]
            List of financial data dictionaries
        """
        dict_payload = {"data-atualizacao": str_date_update, "size": self.int_chunk}
        return self.generic_request(
            "feed/fundos/v2/fundos/serie-historica/lote", 
            str_method="GET", 
            dict_payload=dict_payload
        )

    def funds_registration_data_dt(self, str_date_update: str) -> list[dict[str, Any]]:
        """Retrieve registration data by update date.

        Parameters
        ----------
        str_date_update : str
            Update date in YYYY-MM-DD format

        Returns
        -------
        list[dict[str, Any]]
            List of registration data dictionaries
        """
        dict_payload = {"data-atualizacao": str_date_update, "size": self.int_chunk}
        return self.generic_request(
            "feed/fundos/v2/fundos/dados-cadastrais/lote", 
            str_method="GET", 
            dict_payload=dict_payload
        )

    def institutions(self) -> list[dict[str, Any]]:
        """Retrieve institutions data.

        Returns
        -------
        list[dict[str, Any]]
            List of institution dictionaries
        """
        dict_payload = {"size": self.int_chunk}
        return self.generic_request(
            "feed/fundos/v2/fundos/instituicoes", 
            str_method="GET", 
            dict_payload=dict_payload
        )

    def institution(self, str_ein: str) -> list[dict[str, Any]]:
        """Retrieve specific institution data.

        Parameters
        ----------
        str_ein : str
            Employer Identification Number

        Returns
        -------
        list[dict[str, Any]]
            List of institution data dictionaries
        """
        dict_payload = {"size": self.int_chunk}
        return self.generic_request(
            f"feed/fundos/v2/fundos/instituicoes/{str_ein}", 
            str_method="GET", 
            dict_payload=dict_payload
        )

    def explanatory_notes_fund(self, str_code_class: str) -> list[dict[str, Any]]:
        """Retrieve fund explanatory notes.

        Parameters
        ----------
        str_code_class : str
            Fund class code

        Returns
        -------
        list[dict[str, Any]]
            List of explanatory notes dictionaries
        """
        return self.generic_request(
            f"feed/fundos/v2/fundos/{str_code_class}/notas-explicativas", "GET")