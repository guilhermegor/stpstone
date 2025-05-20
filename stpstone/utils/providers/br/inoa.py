from datetime import datetime
from typing import Any, Dict, List

import backoff
import pandas as pd
from requests import exceptions, request

from stpstone.utils.cals.handling_dates import DatesBR


class AlphaTools:

    def __init__(
        self,
        str_user: str,
        str_passw: str,
        str_host: str,
        str_instance: str,
        dt_start: datetime,
        dt_end: datetime,
        str_fmt_date_output: str = "YYYY-MM-DD",
    ):
        """
        Connection to INOA Alpha Tools API
        Args:
            str_user (str): username
            str_passw (str): password
            str_host (str): host
            str_instance (str): instance
            dt_start (datetime): start date
            dt_end (datetime): end date
            str_fmt_date_output (str): format date output
        Returns:
            None
        """
        self.str_user = str_user
        self.str_passw = str_passw
        self.str_host = str_host
        self.str_instance = str_instance
        self.dt_start = dt_start
        self.dt_end = dt_end
        self.str_fmt_date_output = str_fmt_date_output

    @backoff.on_exception(
        backoff.constant,
        exceptions.RequestException,
        interval=10,
        max_tries=20,
    )
    def generic_req(
        self, str_method: str, str_app: str, dict_params: dict
    ) -> List[Dict[str, Any]]:
        resp_req = request(
            str_method,
            url=self.str_host + str_app,
            json=dict_params,
            auth=(self.str_user, self.str_passw),
        )
        resp_req.raise_for_status()
        return resp_req.json()

    @property
    def funds(self) -> pd.DataFrame:
        dict_params = {
            "values": [
                "id",
                "name",
                "legal_id",
            ],
            "is_active": None,
        }
        json_req = self.generic_req(
            "POST",
            "funds/get_funds",
            dict_params,
        )
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

    def quotes(self, list_ids: List[int]) -> pd.DataFrame:
        dict_params = {
            "fund_ids": list_ids,
            "start_date": self.dt_start.strftime("%Y-%m-%d"),
            "end_date": self.dt_end.strftime("%Y-%m-%d"),
        }
        json_req = self.generic_req(
            "POST",
            "portfolio/get_portfolio_overview_date_range",
            dict_params,
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
            DatesBR().str_date_to_datetime(d, self.str_fmt_date_output)
            for d in df_quotes["date"]
        ]
        df_quotes.columns = [x.upper() for x in df_quotes.columns]
        return df_quotes
