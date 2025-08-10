"""B3 LINE API client implementation.

This module provides a comprehensive interface to interact with B3's LINE API,
including authentication, account management, document handling, and market data
operations. It supports all endpoints documented in the official B3 LINE API guide.

References
----------
.. [1] http://www.b3.com.br/data/files/2E/95/28/F1/EBD17610515A8076AC094EA8/GUIDE-TO-LINE-5.0-API.pdf
.. [2] https://line.bvmfnet.com.br/#/endpoints
"""

from datetime import date, datetime
import time
from typing import Any, Literal, Optional, Union

import pandas as pd
from requests import Response, request

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker
from stpstone.utils.parsers.json import JsonFiles


class ConnectionApi(metaclass=TypeChecker):
    """Base class for B3 LINE API connections.

    Notes
    -----
    Please contact CAU manager account to request a service user.

    Metadata
    --------
    [1] http://www.b3.com.br/data/files/2E/95/28/F1/EBD17610515A8076AC094EA8/GUIDE-TO-LINE-5.0-API.pdf
    [2] https://line.bvmfnet.com.br/#/endpoints
    """

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        broker_code: str,
        category_code: str,
        hostname_api_line_b3: str = "https://api.line.bvmfnet.com.br",
    ) -> None:
        """Initialize API connection with authentication credentials.

        Parameters
        ----------
        client_id : str
            Client identifier for API authentication
        client_secret : str
            Client secret for API authentication
        broker_code : str
            Broker identification code
        category_code : str
            Category classification code
        hostname_api_line_b3 : str, optional
            API base URL (default: "https://api.line.bvmfnet.com.br")
        """
        self._validate_credentials(client_id, client_secret, broker_code, category_code)
        self._validate_url(hostname_api_line_b3)

        self.client_id = client_id
        self.client_secret = client_secret
        self.broker_code = broker_code
        self.category_code = category_code
        self.hostname_api_line_b3 = hostname_api_line_b3
        self.token = self.access_token()

    def _validate_credentials(
        self, client_id: str, client_secret: str, broker_code: str, category_code: str
    ) -> None:
        """Validate authentication credentials.

        Parameters
        ----------
        client_id : str
            Client identifier
        client_secret : str
            Client secret
        broker_code : str
            Broker code
        category_code : str
            Category code

        Raises
        ------
        ValueError
            If any credential is empty
        """
        if not all([client_id, client_secret, broker_code, category_code]):
            raise ValueError("All credentials must be non-empty")

    def _validate_url(self, url: str) -> None:
        """Validate URL format.

        Parameters
        ----------
        url : str
            URL to validate

        Raises
        ------
        ValueError
            If URL is empty or doesn't start with https://
        """
        if not url:
            raise ValueError("URL cannot be empty")
        if not url.startswith("https://"):
            raise ValueError("URL must start with https://")

    def auth_header(
        self, 
        int_max_retries: int = 2, 
        timeout: Union[tuple[float, float], float, int] = 10
    ) -> str:
        """Retrieve authorization header for API authentication.

        Parameters
        ----------
        int_max_retries : int, optional
            Maximum number of retries (default: 2)
        timeout : Union[tuple[float, float], float, int], optional
            Request timeout (default: 10)

        Returns
        -------
        str
            Authorization header value

        Raises
        ------
        ValueError
            If maximum retries exceeded or request fails
        """
        dict_headers = {
            "Accept": "application/json",
            "Content-Type": "application/x-www-form-urlencoded",
        }

        i: int = 0
        int_status_code_iteration: int = 400
        int_status_code_success: int = 200

        while int_status_code_iteration != int_status_code_success and i <= int_max_retries:
            try:
                resp_req = request(
                    method="GET",
                    url=self.hostname_api_line_b3 + "/api/v1.0/token/authorization",
                    headers=dict_headers,
                    verify=False,
                    timeout=timeout,
                )
                int_status_code_iteration = resp_req.status_code
            except Exception:
                i += 1
                continue
            i += 1

        if i > int_max_retries:
            raise ValueError("Maximum retry attempts exceeded")

        resp_req.raise_for_status()
        return resp_req.json()["header"]

    def access_token(
        self,
        int_max_retries: int = 2, 
        timeout: Union[tuple[float, float], float, int] = 10
    ) -> str:
        """Retrieve and manage API access token.

        Parameters
        ----------
        int_max_retries : int, optional
            Maximum number of retries (default: 2)
        timeout : Union[tuple[float, float], float, int], optional
            Request timeout (default: 10)

        Returns
        -------
        str
            Valid access token

        Raises
        ------
        ValueError
            If token retrieval fails or maximum retries exceeded
        """
        str_auth_header = self.auth_header(int_max_retries=int_max_retries, timeout=timeout)
        dict_headers = {"Authorization": f"Basic {str_auth_header}"}
        int_expiration_time = 0
        i_retrieves = 0
        token = ""
        refresh_token = ""
        int_refresh_min_time: int = 4000
        max_retrieves: int = 100
        int_status_code_ok: int = 200
        i_aux: int = 0
        int_status_code_iteration: int = 400

        while int_expiration_time < int_refresh_min_time and i_retrieves < max_retrieves:
            if i_retrieves == 0:
                dict_params = {
                    "grant_type": "password",
                    "username": self.client_id,
                    "password": self.client_secret,
                    "brokerCode": self.broker_code,
                    "categoryCode": self.category_code,
                }
            else:
                dict_params = {"grant_type": "refresh_token", "refresh_token": refresh_token}

            while int_status_code_iteration != int_status_code_ok and i_aux <= max_retrieves:
                try:
                    resp_req = request(
                        method="POST",
                        url=self.hostname_api_line_b3 + "/api/oauth/token",
                        headers=dict_headers,
                        params=dict_params,
                        verify=False,
                        timeout=timeout,
                    )
                    int_status_code_iteration = resp_req.status_code
                except Exception:
                    i_aux += 1
                    continue
                i_aux += 1

            if i_aux > max_retrieves:
                raise ValueError("Maximum retry attempts exceeded")

            resp_req.raise_for_status()
            dict_token = resp_req.json()

            refresh_token = dict_token["refresh_token"]
            token = dict_token["access_token"]
            int_expiration_time = dict_token["expires_in"]
            i_retrieves += 1

        return token

    def app_request(
        self,
        method: Literal["GET", "POST", "DELETE"],
        app: str,
        dict_params: Optional[dict[str, Any]] = None,
        dict_payload: Optional[list[dict[str, Any]]] = None,
        bool_parse_dict_params_data: bool = False,
        bool_retry_if_error: bool = False,
        float_secs_sleep: Optional[float] = None,
        timeout: Union[tuple[float, float], float, int] = 10
    ) -> Union[list[dict[str, Any]], int]:
        """Execute API request with retry logic.

        Parameters
        ----------
        method : Literal['GET', 'POST', 'DELETE']
            HTTP method
        app : str
            API endpoint
        dict_params : Optional[dict[str, Any]], optional
            Request parameters (default: None)
        dict_payload : Optional[list[dict[str, Any]]], optional
            Request payload (default: None)
        bool_parse_dict_params_data : bool, optional
            Parse parameters as JSON (default: False)
        bool_retry_if_error : bool, optional
            Enable retry on error (default: False)
        float_secs_sleep : Optional[float], optional
            Sleep time between retries (default: None)
        timeout : Union[tuple[float, float], float, int], optional
            Request timeout (default: 10)

        Returns
        -------
        Union[list[dict[str, Any]], int]
            Response data or status code

        Raises
        ------
        ValueError
            If request fails after retries
        """
        i: int = 0
        float_secs_sleep_increase_error: float = 1.0
        float_secs_sleep_iteration: float = float_secs_sleep if float_secs_sleep else 1.0
        dict_header: dict[str, str] = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }
        int_max_retries: int = 100
        int_status_code_ok: int = 200
        list_int_http_error_token: list[int] = [401]
        resp_req: Response = None
        bool_retry_request: bool = bool_retry_if_error

        if bool_parse_dict_params_data:
            if dict_params:
                dict_params = JsonFiles().dict_to_json(dict_params)
            if dict_payload:
                dict_payload = JsonFiles().dict_to_json(dict_payload)

        if bool_retry_if_error:
            while bool_retry_request and i <= int_max_retries:
                try:
                    resp_req = request(
                        method=method,
                        url=self.hostname_api_line_b3 + app,
                        headers=dict_header,
                        params=dict_params,
                        data=dict_payload,
                        timeout=timeout,
                    )

                    if resp_req.status_code == int_status_code_ok:
                        bool_retry_request = False
                    elif resp_req.status_code in list_int_http_error_token:
                        dict_header = {
                            "Authorization": f"Bearer {self.access_token()}",
                            "Content-Type": "application/json",
                        }
                    else:
                        if float_secs_sleep_iteration:
                            float_secs_sleep_iteration += float_secs_sleep_increase_error
                except Exception:
                    bool_retry_request = True

                if float_secs_sleep_iteration:
                    time.sleep(float_secs_sleep_iteration)
                i += 1
        else:
            resp_req = request(
                method=method,
                url=self.hostname_api_line_b3 + app,
                headers=dict_header,
                params=dict_params,
                data=dict_payload,
                timeout=timeout,
            )

        if resp_req is None:
            raise ValueError("Request failed after retries")

        resp_req.raise_for_status()

        try:
            return resp_req.json()
        except Exception:
            return resp_req.status_code


class Operations(ConnectionApi):
    """Class for handling market operations through B3 LINE API."""

    def exchange_limits(self) -> Union[list[dict[str, Any]], int]:
        """Retrieve exchange limits for the broker.

        Returns
        -------
        Union[list[dict[str, Any]], int]
            Exchange limits data or status code
        """
        return self.app_request(
            method="GET",
            app=f"/api/v1.0/exchangeLimits/spxi/{self.broker_code}",
            bool_retry_if_error=True,
        )

    def groups_authorized_markets(self) -> Union[list[dict[str, Any]], int]:
        """Retrieve authorized market groups.

        Returns
        -------
        Union[list[dict[str, Any]], int]
            Market groups data or status code
        """
        return self.app_request(
            method="GET", 
            app="/api/v1.0/exchangeLimits/autorizedMarkets", 
            bool_retry_if_error=True
        )

    def intruments_per_group(self, group_id: str) -> Union[list[dict[str, Any]], int]:
        """Retrieve instruments associated with a market group.

        Parameters
        ----------
        group_id : str
            Market group identifier

        Returns
        -------
        Union[list[dict[str, Any]], int]
            Instruments data or status code
        """
        dict_payload = {
            "authorizedMarketGroupId": group_id,
            "isLimitSetted": "true",
        }
        return self.app_request(
            method="POST",
            ap="/api/v1.0/exchangeLimits/findInstruments",
            dict_payload=dict_payload,
            bool_parse_dict_params_data=True,
        )

    def authorized_markets_instruments(self) -> dict[str, Union[str, int, float]]:
        """Retrieve all authorized market instruments with limits.

        Returns
        -------
        dict[str, Union[str, int, float]]
            Dictionary mapping market groups to their instruments and limits
        """
        dict_export: dict[str, Union[str, int, float]] = {}
        json_authorized_markets: Union[list[dict[str, Any]], int] = \
            self.groups_authorized_markets()

        for dict_ in json_authorized_markets:
            for dict_assets in self.intruments_per_group(dict_["id"]):
                if dict_["id"] not in dict_export:
                    dict_export[dict_["id"]] = {}

                dict_export[dict_["id"]]["id"] = dict_["id"]
                dict_export[dict_["id"]]["name"] = dict_["name"]

                if "assets_associated" not in dict_export[dict_["id"]]:
                    dict_export[dict_["id"]]["assets_associated"] = []

                asset_data = {
                    "instrument_symbol": dict_assets["instrumentSymbol"],
                    "instrument_asset": dict_assets["instrumentAsset"],
                    "limit_spci": dict_assets["limitSpci"],
                    "limit_spvi": dict_assets["limitSpvi"],
                }

                if "limitSpciOption" in dict_assets:
                    asset_data.update({
                        "limit_spci_option": dict_assets["limitSpciOption"],
                        "limit_spvi_option": dict_assets["limitSpviOption"],
                    })

                dict_export[dict_["id"]]["assets_associated"].append(asset_data)

        return dict_export


class Resources(Operations):
    """Class for handling market resources through B3 LINE API."""

    def instrument_informations(self) -> Union[list[dict[str, Any]], int]:
        """Retrieve basic instrument information.

        Returns
        -------
        Union[list[dict[str, Any]], int]
            Instrument data or status code
        """
        return self.app_request(
            method="GET", 
            app="/api/v1.0/symbol", 
            bool_retry_if_error=True
        )

    def instrument_infos_exchange_limits(self) -> dict[str, dict[str, Union[str, int, float]]]:
        """Combine instrument info with exchange limits.

        Returns
        -------
        dict[str, dict[str, Union[str, int, float]]]
            Combined instrument data keyed by symbol
        """
        df_exchange_limits = pd.DataFrame.from_dict(self.exchange_limits())
        df_exchange_limits = df_exchange_limits.astype({"instrumentId": str})

        df_instrument_informations = pd.DataFrame.from_dict(self.instrument_informations())
        df_instrument_informations = df_instrument_informations.astype({"id": str})

        df_join_instruments = df_instrument_informations.merge(
            df_exchange_limits,
            how="left",
            left_on="id",
            right_on="instrumentId",
        )

        df_join_instruments = df_join_instruments.rename(
            columns={"symbol_x": "symbol"}
        ).drop(columns=["symbol_y"])

        return {
            row["symbol"]: {col_: row[col_] for col_ in df_join_instruments.columns}
            for _, row in df_join_instruments.iterrows()
        }

    def instrument_id_by_symbol(self, symbol: str) -> Union[list[dict[str, Any]], int]:
        """Retrieve instrument ID by symbol.

        Parameters
        ----------
        symbol : str
            Instrument symbol

        Returns
        -------
        Union[list[dict[str, Any]], int]
            Instrument data or status code
        """
        return self.app_request(
            method="GET", 
            app=f"/api/v1.0/symbol/{symbol}"
        )


class AccountsData(ConnectionApi):
    """Class for handling account data through B3 LINE API."""

    def client_infos(self, account_code: str) -> Union[list[dict[str, Any]], int]:
        """Retrieve client account information.

        Parameters
        ----------
        account_code : str
            Account identifier

        Returns
        -------
        Union[list[dict[str, Any]], int]
            Account data or status code
        """
        dict_params = {
            "participantCode": self.broker_code,
            "pnpCode": self.category_code,
            "accountCode": account_code,
        }
        return self.app_request(
            method="GET",
            app="/api/v1.0/account",
            dict_params=dict_params,
            bool_retry_if_error=True,
        )

    def spxi_get(self, account_id: str) -> Union[list[dict[str, Any]], int]:
        """Retrieve SPCI/SPVI limits for an account.

        Parameters
        ----------
        account_id : str
            Account identifier

        Returns
        -------
        Union[list[dict[str, Any]], int]
            Limits data or status code
        """
        dict_params = {"accId": account_id}
        return self.app_request(
            method="GET", 
            app=f"/api/v1.0/account/{account_id}/lmt/spxi", 
            dict_params=dict_params
        )

    def spxi_instrument_post(
        self,
        account_id: str,
        dict_payload: Optional[list[dict[str, Any]]]
    ) -> Union[list[dict[str, Any]], int]:
        """Set SPCI/SPVI limits for account instruments.

        Parameters
        ----------
        account_id : str
            Account identifier
        dict_payload : Optional[list[dict[str, Any]]]
            list of instrument limit dictionaries

        Returns
        -------
        Union[list[dict[str, Any]], int]
            Result data or status code
        """
        return self.app_request(
            method="POST",
            app=f"/api/v1.0/account/{account_id}/lmt/spxi",
            dict_payload=dict_payload,
            bool_parse_dict_params_data=True,
        )

    def spxi_instrument_delete(
        self,
        account_id: str,
        dict_payload: Optional[list[dict[str, Any]]],
    ) -> Union[list[dict[str, Any]], int]:
        """Remove SPCI/SPVI limits for account instruments.

        Parameters
        ----------
        account_id : str
            Account identifier
        dict_payload : Optional[list[dict[str, Any]]]
            list of instrument removal dictionaries

        Returns
        -------
        Union[list[dict[str, Any]], int]
            Result data or status code
        """
        return self.app_request(
            method="POST",
            app=f"/api/v1.0/account/{account_id}/lmt/spxi",
            dict_payload=dict_payload,
            bool_parse_dict_params_data=True,
        )

    def spxi_tmox_global_metrics_remove(
        self,
        account_id: str
    ) -> Union[list[dict[str, Any]], int]:
        """Remove all global metrics limits for an account.

        Parameters
        ----------
        account_id : str
            Account identifier

        Returns
        -------
        Union[list[dict[str, Any]], int]
            Result data or status code
        """
        dict_params = {"accId": account_id}
        return self.app_request(
            method="DELETE", 
            app=f"/api/v1.0/account/{account_id}/lmt", 
            dict_params=dict_params
        )

    def specific_global_metric_remotion(
        self,
        account_id: str,
        metric: str,
    ) -> Union[list[dict[str, Any]], int]:
        """Remove specific global metric limit for an account.

        Parameters
        ----------
        account_id : str
            Account identifier
        metric : str
            Metric identifier to remove

        Returns
        -------
        Union[list[dict[str, Any]], int]
            Result data or status code
        """
        dict_params = {"accId": account_id, "metric": metric}
        return self.app_request(
            method="DELETE", 
            app=f"/api/v2.0/account/{account_id}/lmt", 
            dict_params=dict_params
        )


class DocumentsData(ConnectionApi):
    """Class for handling document operations through B3 LINE API."""

    def doc_info(
        self,
        doc_code: str
    ) -> Union[list[dict[str, Any]], int]:
        """Retrieve document information by code.

        Parameters
        ----------
        doc_code : str
            Document identifier

        Returns
        -------
        Union[list[dict[str, Any]], int]
            Document data or status code
        """
        dict_params = {
            "participantCode": self.broker_code,
            "pnpCode": self.category_code,
            "documentCode": str(doc_code),
        }
        return self.app_request(
            method="GET",
            app="/api/v1.0/document",
            dict_params=dict_params,
            bool_retry_if_error=True,
        )

    def block_unblock_doc(
        self,
        doc_id: str,
    ) -> Union[list[dict[str, Any]], int]:
        """Block or unblock a document.

        Parameters
        ----------
        doc_id : str
            Document identifier

        Returns
        -------
        Union[list[dict[str, Any]], int]
            Result data or status code
        """
        dict_params = {"id": str(doc_id), "isBlocked": True}
        return self.app_request(
            method="POST",
            app=f"/api/v1.0/document/{doc_id}",
            dict_params=dict_params,
            bool_parse_dict_params_data=True,
            bool_retry_if_error=True,
        )

    def update_profile(
        self,
        doc_id: str,
        doc_profile_id: str,
        int_rmkt_evaluation: int = 0,
    ) -> Union[list[dict[str, Any]], int]:
        """Update document risk profile.

        Parameters
        ----------
        doc_id : str
            Document identifier
        doc_profile_id : str
            New profile identifier
        int_rmkt_evaluation : int, optional
            Risk market evaluation (default: 0)

        Returns
        -------
        Union[list[dict[str, Any]], int]
            Result data or status code
        """
        dict_payload = {
            "id": str(doc_id),
            "profileFull": int(doc_profile_id),
            "rmktEvaluation": int_rmkt_evaluation,
        }
        return self.app_request(
            method="POST",
            app=f"/api/v1.0/document/{str(doc_id)}",
            dict_payload=dict_payload,
            bool_parse_dict_params_data=True,
            bool_retry_if_error=True,
        )

    def is_protection_mode(
        self,
        doc_id: str,
    ) -> Union[list[dict[str, Any]], int]:
        """Set document protection mode.

        Parameters
        ----------
        doc_id : str
            Document identifier

        Returns
        -------
        Union[list[dict[str, Any]], int]
            Result data or status code
        """
        dict_payload = {
            "id": str(doc_id),
            "isProtected": "true",
        }
        return self.app_request(
            method="POST",
            app=f"/api/v1.0/document/{doc_id}",
            dict_payload=dict_payload,
            bool_parse_dict_params_data=True,
            bool_retry_if_error=True,
        )

    def client_infos(
        self,
        doc_id: str,
    ) -> Union[list[dict[str, Any]], int]:
        """Retrieve client information by document ID.

        Parameters
        ----------
        doc_id : str
            Document identifier

        Returns
        -------
        Union[list[dict[str, Any]], int]
            Client data or status code
        """
        dict_params = {
            "participantCode": self.broker_code,
            "pnpCode": self.category_code,
            "documentId": doc_id,
        }
        return self.app_request(
            method="GET",
            app="/api/v1.0/account",
            dict_params=dict_params,
            bool_retry_if_error=True,
        )

    def doc_profile(
        self,
        doc_id: str,
    ) -> dict[str, Union[str, int]]:
        """Retrieve document profile information.

        Parameters
        ----------
        doc_id : str
            Document identifier

        Returns
        -------
        dict[str, Union[str, int]]
            Profile ID and name
        """
        json_doc = self.app_request(
            method="GET", 
            app=f"/api/v2.0/document/v2.0/document/{doc_id}", 
            bool_retry_if_error=True
        )
        return {
            "profile_id": json_doc["profileFull"],
            "profile_name": json_doc["profileName"],
        }

    def spxi_get(
        self,
        doc_id: str,
    ) -> Union[list[dict[str, Any]], int]:
        """Retrieve SPCI/SPVI limits for a document.

        Parameters
        ----------
        doc_id : str
            Document identifier

        Returns
        -------
        Union[list[dict[str, Any]], int]
            Limits data or status code
        """
        dict_params = {"docId": doc_id}
        return self.app_request(
            method="GET", 
            app=f"/api/v1.0/document/{doc_id}/lmt/spxi", 
            dict_params=dict_params
        )

    def spxi_instrument_post(
        self,
        doc_id: str,
        dict_payload: Optional[list[dict[str, Any]]],
    ) -> Union[list[dict[str, Any]], int]:
        """Set SPCI/SPVI limits for document instruments.

        Parameters
        ----------
        doc_id : str
            Document identifier
        dict_payload : Optional[list[dict[str, Any]]]
            list of instrument limit dictionaries

        Returns
        -------
        Union[list[dict[str, Any]], int]
            Result data or status code
        """
        return self.app_request(
            method="POST",
            app=f"/api/v1.0/document/{doc_id}/lmt/spxi",
            dict_payload=dict_payload,
            bool_parse_dict_params_data=True,
            bool_retry_if_error=True,
        )

    def spxi_instrument_delete(
        self,
        doc_id: str,
        dict_payload: Optional[list[dict[str, Any]]],
    ) -> Union[list[dict[str, Any]], int]:
        """Remove SPCI/SPVI limits for document instruments.

        Parameters
        ----------
        doc_id : str
            Document identifier
        dict_payload : Optional[list[dict[str, Any]]]
            list of instrument removal dictionaries

        Returns
        -------
        Union[list[dict[str, Any]], int]
            Result data or status code
        """
        return self.app_request(
            method="POST",
            app=f"/api/v1.0/document/{doc_id}/lmt/spxi",
            dict_payload=dict_payload,
            bool_parse_dict_params_data=True,
        )


class Professional(ConnectionApi):
    """Class for handling professional data through B3 LINE API."""

    def professional_code_get(self) -> Union[list[dict[str, Any]], int]:
        """Retrieve professional information by code.

        Returns
        -------
        Union[list[dict[str, Any]], int]
            Professional data or status code
        """
        dict_params = {
            "participantCode": self.broker_code,
            "pnpCode": self.category_code,
        }
        return self.app_request(
            method="GET", 
            app="/api/v1.0/operationsProfessionalParticipant/code", 
            dict_params=dict_params
        )

    def professional_historic_position(
        self,
        professional_code: str,
        dt_start: Union[datetime, date],
        dt_end: Union[datetime, date],
        int_participant_perspective_type: int = 0,
        entity_type: int = 4,
    ) -> Union[list[dict[str, Any]], int]:
        """Retrieve professional position history.

        Parameters
        ----------
        professional_code : str
            Professional identifier
        dt_start : Union[datetime, date]
            Start date
        dt_end : Union[datetime, date]
            End date
        int_participant_perspective_type : int, optional
            Participant perspective type, by default 0
        entity_type : int, optional
            Entity type, by default 4

        Returns
        -------
        Union[list[dict[str, Any]], int]
            Position history data or status code
        """
        list_metric_type: list[int] = [1, 2, 3, 4, 6, 7, 22, 25, 26, 27, 28, 29, 36, 38, 39]
        int_items_per_page: int = 50

        dict_payload = {
            "angularItensPerPage": int_items_per_page,
            "entityType": entity_type,
            "metricTypes": list_metric_type,
            "ownerBrokerCode": int(self.broker_code),
            "ownerCategoryType": int(self.category_code),
            "partPerspecType": int_participant_perspective_type,
            "registryDateEnd": dt_end.strftime("%Y-%m-%d"),
            "registryDateStart": dt_start.strftime("%Y-%m-%d"),
            "traderCode": professional_code,
        }

        return self.app_request(
            method="POST",
            app="https://api.line.trd.cert.bvmfnet.com.br/api/v2.0/position/hstry",
            dict_payload=dict_payload,
            bool_retry_if_error=True,
            bool_parse_dict_params_data=True,
        )


class ProfilesData(ConnectionApi):
    """Class for handling risk profile data through B3 LINE API."""

    def risk_profile(self) -> Union[list[dict[str, Any]], int]:
        """Retrieve available risk profiles.

        Returns
        -------
        Union[list[dict[str, Any]], int]
            Profile data or status code
        """
        return self.app_request(
            method="GET", 
            app="/api/v1.0/riskProfile"
        )

    def entities_associated_profile(
        self,
        id_profile: str,
    ) -> Union[list[dict[str, Any]], int]:
        """Retrieve entities associated with a risk profile.

        Parameters
        ----------
        id_profile : str
            Profile identifier

        Returns
        -------
        Union[list[dict[str, Any]], int]
            Entity data or status code
        """
        dict_params = {
            "id": id_profile,
            "participantCode": self.broker_code,
            "pnpCode": self.category_code,
        }
        return self.app_request(
            method="GET",
            app="/api/v1.0/riskProfile/enty",
            dict_params=dict_params,
            bool_retry_if_error=True,
        )

    def profile_global_limits_get(
        self,
        prof_id: str,
    ) -> Union[list[dict[str, Any]], int]:
        """Retrieve global limits for a risk profile.

        Parameters
        ----------
        prof_id : str
            Profile identifier

        Returns
        -------
        Union[list[dict[str, Any]], int]
            Limits data or status code
        """
        return self.app_request(
            method="GET", 
            app=f"/api/v1.0/riskProfile/{prof_id}/lmt"
        )

    def profile_market_limits_get(
        self,
        prof_id: str,
    ) -> Union[list[dict[str, Any]], int]:
        """Retrieve market limits for a risk profile.

        Parameters
        ----------
        prof_id : str
            Profile identifier

        Returns
        -------
        Union[list[dict[str, Any]], int]
            Limits data or status code
        """
        return self.app_request(
            method="GET",
            app=f"/api/v1.0/riskProfile/{prof_id}/lmt/mkta",
            bool_retry_if_error=True,
        )

    def profile_spxi_limits_get(
        self,
        prof_id: str,
    ) -> Union[list[dict[str, Any]], int]:
        """Retrieve SPCI/SPVI limits for a risk profile.

        Parameters
        ----------
        prof_id : str
            Profile identifier

        Returns
        -------
        Union[list[dict[str, Any]], int]
            Limits data or status code
        """
        return self.app_request(
            method="GET", 
            app=f"/api/v1.0/riskProfile/{prof_id}/lmt/spxi"
        )

    def profile_tmox_limits_get(
        self,
        prof_id: str,
    ) -> Union[list[dict[str, Any]], int]:
        """Retrieve TMOC/TMOV limits for a risk profile.

        Parameters
        ----------
        prof_id : str
            Profile identifier

        Returns
        -------
        Union[list[dict[str, Any]], int]
            Limits data or status code
        """
        return self.app_request(
            method="GET", 
            app=f"/api/v1.0/riskProfile/{prof_id}/lmt/tmox"
        )

    def spxi_instrument_post(
        self,
        prof_id: str,
        dict_payload: list[dict[str, Any]],
    ) -> Union[list[dict[str, Any]], int]:
        """Set TMOC/TMOV limits for profile instruments.

        Parameters
        ----------
        prof_id : str
            Profile identifier
        dict_payload : list[dict[str, Any]]
            list of instrument limit dictionaries

        Returns
        -------
        Union[list[dict[str, Any]], int]
            Result data or status code
        """
        return self.app_request(
            method="POST",
            app=f"/api/v1.0/riskProfile/{prof_id}/lmt/tmox",
            dict_payload=dict_payload,
            bool_parse_dict_params_data=True,
            bool_retry_if_error=True,
        )


class Monitoring(ConnectionApi):
    """Class for handling monitoring alerts through B3 LINE API."""

    def alerts(self) -> Union[list[dict[str, Any]], int]:
        """Retrieve latest monitoring alerts.

        Returns
        -------
        Union[list[dict[str, Any]], int]
            Alert data or status code
        """
        return self.app_request(
            method="GET", 
            app="/api/v1.0/alert/lastalerts?filterRead=true", 
            bool_retry_if_error=True
        )


class SystemEventManagement(ConnectionApi):
    """Class for handling system events through B3 LINE API."""

    def report(
        self,
        dt_start: Union[datetime, date],
        dt_end: Union[datetime, date],
        str_start_time: str = "00:00",
        str_sup_time: str = "23:59",
        int_entity_type: int = 3,
    ) -> Union[list[dict[str, Any]], int]:
        """Generate system event report.

        Parameters
        ----------
        dt_start : Union[datetime, date]
            Start date
        dt_end : Union[datetime, date]
            End date
        str_start_time : str, optional
            Start time, by default "00:00"
        str_sup_time : str, optional
            End time, by default "23:59"
        int_entity_type : int, optional
            Entity type, by default 3

        Returns
        -------
        Union[list[dict[str, Any]], int]
            Report data or status code
        """
        dict_payload = {
            "participantCode": int(self.broker_code),
            "categoryType": int(self.category_code),
            "entityType": int_entity_type,
            "carryingAccountCode": "null",
            "pnpCode": "",
            "accountTypeLineDomain": "null",
            "ownerName": "null",
            "documentCode": "null",
            "accountCode": "null",
            "startTime": str_start_time,
            "endTime": str_sup_time,
            "startDate": dt_start.strftime("%d/%m/%Y"),
            "endDate": dt_end.strftime("%d/%m/%Y"),
        }
        return self.app_request(
            method="POST",
            app="/api/v1.0/systemEvent",
            dict_payload=dict_payload,
            bool_parse_dict_params_data=True,
        )