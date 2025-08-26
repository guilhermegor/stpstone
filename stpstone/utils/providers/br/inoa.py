"""INOA Alpha Tools API interface for financial data retrieval.

This module provides a class for interacting with the INOA Alpha Tools API to fetch
fund information and quotes data. It handles authentication, request retries, and
data formatting using pandas DataFrames.
"""

from datetime import datetime
from typing import Any, Literal, TypedDict

import backoff
import pandas as pd
from requests import exceptions, request

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker
from stpstone.utils.calendars.calendar_abc import DatesBR


class ReturnGenericReq(TypedDict):
    """Typed dictionary for generic request response.

    Attributes
    ----------
    items : list[dict[str, Any]]
        List of items returned from the API
    """

    items: list[dict[str, Any]]


class AlphaTools(metaclass=TypeChecker):
    """Client for INOA Alpha Tools API operations."""

    def __init__(
        self,
        str_user: str,
        str_passw: str,
        str_host: str,
        str_instance: str,
        date_start: datetime,
        date_end: datetime,
        str_fmt_date_output: str = "YYYY-MM-DD",
    ) -> None:
        """Initialize AlphaTools API client.

        Parameters
        ----------
        str_user : str
            Username for API authentication
        str_passw : str
            Password for API authentication
        str_host : str
            Base host URL for API endpoints
        str_instance : str
            Instance identifier for data origin tracking
        date_start : datetime
            Start date for data queries
        date_end : datetime
            End date for data queries
        str_fmt_date_output : str, optional
            Format string for output dates (default: "YYYY-MM-DD")
        """
        self._validate_string_params(str_user, str_passw, str_host, str_instance)
        self._validate_dates(date_start, date_end)

        self.str_user = str_user
        self.str_passw = str_passw
        self.str_host = str_host
        self.str_instance = str_instance
        self.date_start = date_start
        self.date_end = date_end
        self.str_fmt_date_output = str_fmt_date_output
        self.cls_dates_br = DatesBR()

    def _validate_string_params(
        self,
        str_user: str,
        str_passw: str,
        str_host: str,
        str_instance: str,
    ) -> None:
        """Validate string parameters are non-empty.

        Parameters
        ----------
        str_user : str
            Username to validate
        str_passw : str
            Password to validate
        str_host : str
            Host URL to validate
        str_instance : str
            Instance identifier to validate

        Raises
        ------
        ValueError
            If any parameter is empty or not a string
        """
        for name, value in [
            ("str_user", str_user),
            ("str_passw", str_passw),
            ("str_host", str_host),
            ("str_instance", str_instance),
        ]:
            if not value:
                raise ValueError(f"{name} cannot be empty")
            if not isinstance(value, str):
                raise ValueError(f"{name} must be a string")

    def _validate_dates(self, date_start: datetime, date_end: datetime) -> None:
        """Validate date range parameters.

        Parameters
        ----------
        date_start : datetime
            Start date to validate
        date_end : datetime
            End date to validate

        Raises
        ------
        ValueError
            If start date is after end date
            If dates are not datetime objects
        """
        if not isinstance(date_start, datetime) or not isinstance(date_end, datetime):
            raise ValueError("Dates must be datetime objects")
        if date_start > date_end:
            raise ValueError("Start date cannot be after end date")

    @backoff.on_exception(
        backoff.constant,
        exceptions.RequestException,
        interval=10,
        max_tries=20,
    )
    def generic_req(
        self,
        str_method: Literal["GET", "POST"],
        str_app: str,
        dict_params: dict[str, Any],
    ) -> list[dict[str, Any]]:
        """Make generic API request with retry logic.

        Parameters
        ----------
        str_method : Literal['GET', 'POST']
            HTTP method to use
        str_app : str
            API endpoint path
        dict_params : dict[str, Any]
            Parameters to send with request

        Returns
        -------
        list[dict[str, Any]]
            Parsed JSON response from API

        Raises
        ------
        ValueError
            If request fails after max retries
        """
        try:
            resp_req = request(
                str_method,
                url=self.str_host + str_app,
                json=dict_params,
                auth=(self.str_user, self.str_passw),
                timeout=(200, 200),
            )
            resp_req.raise_for_status()
            return resp_req.json()
        except Exception as err:
            raise ValueError(f"API request failed: {str(err)}") from err

    @property
    def funds(self) -> pd.DataFrame:
        """Retrieve funds information from API.

        Returns
        -------
        pd.DataFrame
            DataFrame containing fund IDs, names, legal IDs and origin

        Raises
        ------
        ValueError
            If API response cannot be parsed
        """
        dict_params = {
            "values": ["id", "name", "legal_id"],
            "is_active": None,
        }
        try:
            json_req = self.generic_req("POST", "funds/get_funds", dict_params)
            df_funds = pd.DataFrame.from_dict(json_req, orient="index")
            df_funds = df_funds.astype(
                {
                    "id": int,
                    "name": str,
                    "legal_id": str,
                }
            )
            df_funds["origin"] = self.str_instance
            df_funds.columns = [x.upper() for x in df_funds.columns]
            return df_funds
        except Exception as err:
            raise ValueError(f"Failed to process funds data: {str(err)}") from err

    def quotes(self, list_ids: list[int]) -> pd.DataFrame:
        """Retrieve quotes for given fund IDs within date range.

        Parameters
        ----------
        list_ids : list[int]
            List of fund IDs to query

        Returns
        -------
        pd.DataFrame
            DataFrame containing quotes data with standardized column names

        Raises
        ------
        ValueError
            If fund ID list is empty
            If API response is malformed
        """
        if not list_ids:
            raise ValueError("Fund ID list cannot be empty")

        dict_params = {
            "fund_ids": list_ids,
            "start_date": self.date_start.strftime("%Y-%m-%d"),
            "end_date": self.date_end.strftime("%Y-%m-%d"),
        }
        try:
            json_req = self.generic_req(
                "POST", "portfolio/get_portfolio_overview_date_range", dict_params
            )
            df_quotes = pd.DataFrame(json_req["items"])
            df_quotes = df_quotes.astype(
                {
                    "fund_id": int,
                    "date": str,
                    "status_display": str,
                }
            )
            df_quotes["date"] = [
                self.cls_dates_br.str_date_to_datetime(d, self.str_fmt_date_output)
                for d in df_quotes["date"]
            ]
            df_quotes.columns = [x.upper() for x in df_quotes.columns]
            return df_quotes
        except Exception as err:
            raise ValueError(f"Failed to process quotes data: {str(err)}") from err