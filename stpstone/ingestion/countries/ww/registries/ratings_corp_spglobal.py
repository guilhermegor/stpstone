"""S&P Global corporate ratings ingestion."""

from datetime import date
from logging import Logger
import re
from time import sleep
from typing import Optional, Union

import backoff
import pandas as pd
from playwright.sync_api import Page as PlaywrightPage
import requests
from requests import Response, Session
from selenium.webdriver.remote.webdriver import WebDriver as SeleniumWebDriver

from stpstone.ingestion.abc.ingestion_abc import (
    ABCIngestionOperations,
    ContentParser,
    CoreIngestion,
)
from stpstone.utils.calendars.calendar_abc import DatesCurrent
from stpstone.utils.calendars.calendar_br import DatesBRAnbima
from stpstone.utils.loggs.create_logs import CreateLog
from stpstone.utils.parsers.folders import DirFilesManagement
from stpstone.utils.webdriver_tools.selenium_wd import SeleniumWD


class RatingsCorpSPGlobalOnePage(ABCIngestionOperations):
    """S&P Global corporate rating actions ingestion for a single result page."""

    _API_HOST = "https://ratings-ext-api-awse.prod.api.spglobal.com/"
    _APP_PATH = "spcom-disclosureapi/extoauthv2/getRatingActionsRequest"
    _TOKEN_URL = (
        "https://disclosure-assets-mfe.prod.cdnratings.spglobal.com/config.json"  # noqa: S105
    )
    _BEARER_URL = (
        "https://disclosure.spglobal.com/ratings/en/regulatory/ratings-actions"
    )
    _BEARER_REGEX = r"(?i)system_access_token=([^*]+?);"
    _HEADERS_BASE = {
        "accept": "application/json, text/plain, */*",
        "accept-language": "en-US,en;q=0.9,pt;q=0.8,es;q=0.7",
        "access-control-allow-origin": "*",
        "content-type": "application/json; charset=UTF-8",
        "origin": "https://disclosure.spglobal.com",
        "referer": "https://disclosure.spglobal.com/",
        "user-agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36"
        ),
    }

    def __init__(
        self,
        bearer: str,
        token: Optional[str] = None,
        pg_number: int = 1,
        date_ref: Optional[date] = None,
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
    ) -> None:
        """Initialize the ingestion class.

        Parameters
        ----------
        bearer : str
            Authorization bearer token (obtained via get_bearer).
        token : Optional[str], optional
            S&P Global API key; fetched from config.json if None, by default None.
        pg_number : int, optional
            The result page number (1-indexed), by default 1.
        date_ref : Optional[date], optional
            The date of reference, by default None.
        logger : Optional[Logger], optional
            The logger, by default None.
        cls_db : Optional[Session], optional
            The database session, by default None.

        Returns
        -------
        None
        """
        super().__init__(cls_db=cls_db)
        CoreIngestion.__init__(self)
        ContentParser.__init__(self)

        self.bearer = bearer
        self.pg_number = pg_number
        self.logger = logger
        self.cls_db = cls_db
        self.cls_dir_files_management = DirFilesManagement()
        self.cls_dates_current = DatesCurrent()
        self.cls_create_log = CreateLog()
        self.cls_dates_br = DatesBRAnbima()
        self.date_ref = date_ref or \
            self.cls_dates_br.add_working_days(self.cls_dates_current.curr_date(), -1)
        self.token = token or self._fetch_token()
        self.url = f"{self._API_HOST}{self._APP_PATH}?apikey={self.token}"

    def run(
        self,
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = False,
        bool_insert_or_ignore: bool = False,
        str_table_name: str = "ww_spglobal_ratings_corp",
    ) -> Optional[pd.DataFrame]:
        """Run the ingestion process for a single page.

        If the database session is provided, the data is inserted into the database.
        Otherwise, the transformed DataFrame is returned.

        Parameters
        ----------
        timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]], optional
            The timeout, by default (12.0, 21.0).
        bool_verify : bool, optional
            Whether to verify the SSL certificate, by default False.
        bool_insert_or_ignore : bool, optional
            Whether to insert or ignore the data, by default False.
        str_table_name : str, optional
            The name of the table, by default 'ww_spglobal_ratings_corp'.

        Returns
        -------
        Optional[pd.DataFrame]
            The transformed DataFrame.
        """
        resp_req = self.get_response(timeout=timeout, bool_verify=bool_verify)
        df_ = self.transform_data(resp_req=resp_req)
        df_ = self.standardize_dataframe(
            df_=df_,
            date_ref=self.date_ref,
            dict_dtypes={
                "RATING_ACTION_DATE": str,
                "ACTION_TYPE_CODE": str,
                "ENTITY_ID": int,
                "SOURCE_PROVIDED_NAME": str,
                "ACTION_LEVEL_INDICATOR": "category",
                "ACTION_NAME": str,
                "SECTOR_CODE": "category",
                "RATING_FROM": "category",
                "RATING_TO": "category",
                "RATING_TYPE": "category",
                "MATURITY_DATE": str,
                "ID": int,
            },
            cols_from_case="camel",
            cols_to_case="upper_constant",
            str_fmt_dt="YYYY-MM-DD",
            url=self.url,
        )
        if self.cls_db:
            self.insert_table_db(
                cls_db=self.cls_db,
                str_table_name=str_table_name,
                df_=df_,
                bool_insert_or_ignore=bool_insert_or_ignore,
            )
        else:
            return df_

    @backoff.on_exception(backoff.expo, requests.exceptions.HTTPError, max_time=60)
    def get_response(
        self,
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = False,
    ) -> Union[Response, PlaywrightPage, SeleniumWebDriver]:
        """Post a rating-actions request and return the response.

        Parameters
        ----------
        timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]], optional
            The timeout, by default (12.0, 21.0).
        bool_verify : bool, optional
            Verify the SSL certificate, by default False.

        Returns
        -------
        Union[Response, PlaywrightPage, SeleniumWebDriver]
            A response object.
        """
        headers = {**self._HEADERS_BASE, "authorization": self.bearer}
        payload = {
            "numberOfDays": "7",
            "pageLength": "100",
            "pageNumber": str(self.pg_number),
            "rd5Group": "",
            "countryName": "",
            "actionType": "",
            "locale": "en_US",
            "urlParam": "",
            "jpSectorWebId": "",
        }
        resp_req = requests.post(
            self.url,
            json=payload,
            headers=headers,
            timeout=timeout,
            verify=bool_verify,
        )
        resp_req.raise_for_status()
        return resp_req

    def parse_raw_file(
        self,
        resp_req: Union[Response, PlaywrightPage, SeleniumWebDriver],
    ) -> Union[Response, PlaywrightPage, SeleniumWebDriver]:
        """Pass through the response object for JSON parsing in transform_data.

        Parameters
        ----------
        resp_req : Union[Response, PlaywrightPage, SeleniumWebDriver]
            The response object.

        Returns
        -------
        Union[Response, PlaywrightPage, SeleniumWebDriver]
            The same response object, unchanged.
        """
        return resp_req

    def transform_data(
        self,
        resp_req: Union[Response, PlaywrightPage, SeleniumWebDriver],
    ) -> pd.DataFrame:
        """Extract rating actions from the JSON response.

        Parameters
        ----------
        resp_req : Union[Response, PlaywrightPage, SeleniumWebDriver]
            The response object with a JSON body containing a 'RatingAction' key.

        Returns
        -------
        pd.DataFrame
            The transformed DataFrame of rating actions.
        """
        return pd.DataFrame(resp_req.json()["RatingAction"])

    @property
    def get_bearer(self) -> str:
        """Obtain a bearer token by inspecting Selenium network traffic.

        Returns
        -------
        str
            The bearer token string, prefixed with 'Bearer '.
        """
        cls_selenium = SeleniumWD(self._BEARER_URL, bool_headless=True, bool_incognito=True)
        cls_selenium.wait(60)
        cls_selenium.wait_until_el_loaded(
            '//a[@class="link-black link-black-hover text-underline"]'
        )
        sleep(60)
        list_network_traffic = cls_selenium.get_network_traffic
        int_idx_bearer = next(
            i
            for i, d in enumerate(list_network_traffic)
            if d["method"] == "Network.responseReceivedExtraInfo"
        )
        regex_match = re.search(
            self._BEARER_REGEX,
            list_network_traffic[int_idx_bearer]["params"]["headers"]["set-cookie"],
        )
        if (
            regex_match is not None
            and regex_match.group(0) is not None
            and len(regex_match.group(0)) > 0
        ):
            return f"Bearer {regex_match.group(1)}"
        raise RuntimeError("Bearer token not found in network traffic.")

    def _fetch_token(self) -> str:
        """Fetch the S&P Global API key from the public config endpoint.

        Returns
        -------
        str
            The API key string.
        """
        token_headers = {
            "sec-ch-ua-platform": '"Windows"',
            "Referer": "https://disclosure.spglobal.com/",
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36"
            ),
            "Accept": "application/json, text/plain, */*",
        }
        resp = requests.get(self._TOKEN_URL, headers=token_headers, timeout=(12.0, 21.0))
        resp.raise_for_status()
        return resp.json()["apiKey"]


class RatingsCorpSPGlobal:
    """Orchestrator that paginates through all S&P Global corporate rating actions."""

    def __init__(
        self,
        date_ref: Optional[date] = None,
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
    ) -> None:
        """Initialize the orchestrator.

        Parameters
        ----------
        date_ref : Optional[date], optional
            The date of reference forwarded to each page ingestion, by default None.
        logger : Optional[Logger], optional
            The logger, by default None.
        cls_db : Optional[Session], optional
            The database session, by default None.

        Returns
        -------
        None
        """
        self.date_ref = date_ref
        self.logger = logger
        self.cls_db = cls_db
        self.cls_create_log = CreateLog()

    @property
    def get_corp_ratings(self) -> pd.DataFrame:
        """Fetch all corporate rating actions across all available pages.

        Returns
        -------
        pd.DataFrame
            Combined DataFrame of all rating actions.
        """
        list_ser: list[dict] = []
        str_bearer = RatingsCorpSPGlobalOnePage(
            bearer="", date_ref=self.date_ref, logger=self.logger
        ).get_bearer
        for i in range(1, 100):
            try:
                cls_ = RatingsCorpSPGlobalOnePage(
                    bearer=str_bearer,
                    pg_number=i,
                    date_ref=self.date_ref,
                    logger=self.logger,
                )
                df_ = cls_.run()
                if df_ is None or df_.empty:
                    break
                list_ser.extend(df_.to_dict(orient="records"))
                sleep(10)
            except Exception as e:
                self.cls_create_log.log_message(
                    self.logger, f"Pagination stopped at page {i}: {e}", log_level="warning"
                )
                break
        return pd.DataFrame(list_ser)

    def update_db(self) -> None:
        """Insert all corporate rating actions into the database, page by page.

        Returns
        -------
        None
        """
        str_bearer = RatingsCorpSPGlobalOnePage(
            bearer="", date_ref=self.date_ref, logger=self.logger
        ).get_bearer
        for i in range(1, 100):
            try:
                cls_ = RatingsCorpSPGlobalOnePage(
                    bearer=str_bearer,
                    pg_number=i,
                    date_ref=self.date_ref,
                    logger=self.logger,
                    cls_db=self.cls_db,
                )
                cls_.run()
                sleep(10)
            except Exception as e:
                self.cls_create_log.log_message(
                    self.logger, f"DB update stopped at page {i}: {e}", log_level="warning"
                )
                break
