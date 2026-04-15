"""Base class for Global-Rates.com interest rates and inflation data ingestion."""

from datetime import date
from logging import Logger
from typing import Any, Optional, Union

import backoff
from bs4 import BeautifulSoup
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
from stpstone.utils.parsers.dicts import HandlingDicts
from stpstone.utils.parsers.folders import DirFilesManagement
from stpstone.utils.parsers.html import HtmlHandler
from stpstone.utils.parsers.lists import ListHandler
from stpstone.utils.parsers.numbers import NumHandler
from stpstone.utils.parsers.str import StrHandler


class _GlobalRatesBase(ABCIngestionOperations):
    """Base class with shared HTML-parsing logic for all Global-Rates.com sources."""

    _BASE_HOST = "https://www.global-rates.com/"
    _SOURCE_NAME: str = ""
    _TABLE_NAME: str = ""
    _DTYPES: dict[str, Any] = {}
    _STR_FMT_DT: str = "MM-DD-YYYY"
    _PATH: str = ""
    _HEADERS: dict[str, str] = {
        "accept": (
            "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,"
            "image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7"
        ),
        "accept-language": "en-US,en;q=0.9,pt;q=0.8,es;q=0.7",
        "user-agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36"
        ),
        "upgrade-insecure-requests": "1",
    }

    def __init__(
        self,
        date_ref: Optional[date] = None,
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
    ) -> None:
        """Initialize the base ingestion class.

        Parameters
        ----------
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

        self.logger = logger
        self.cls_db = cls_db
        self.cls_dir_files_management = DirFilesManagement()
        self.cls_dates_current = DatesCurrent()
        self.cls_create_log = CreateLog()
        self.cls_dates_br = DatesBRAnbima()
        self.date_ref = date_ref or \
            self.cls_dates_br.add_working_days(self.cls_dates_current.curr_date(), -1)
        self.url = f"{self._BASE_HOST}{self._PATH}"

    def run(
        self,
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = False,
        bool_insert_or_ignore: bool = False,
        str_table_name: Optional[str] = None,
    ) -> Optional[pd.DataFrame]:
        """Run the ingestion process.

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
        str_table_name : Optional[str], optional
            The name of the table, by default the class-level _TABLE_NAME.

        Returns
        -------
        Optional[pd.DataFrame]
            The transformed DataFrame.
        """
        str_table_name = str_table_name or self._TABLE_NAME
        resp_req = self.get_response(timeout=timeout, bool_verify=bool_verify)
        df_ = self.transform_data(resp_req=resp_req)
        df_ = self.standardize_dataframe(
            df_=df_,
            date_ref=self.date_ref,
            dict_dtypes=self._DTYPES,
            cols_to_case="upper_constant",
            str_fmt_dt=self._STR_FMT_DT,
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
        """Return an HTML response from Global-Rates.com.

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
        resp_req = requests.get(
            self.url,
            headers=self._HEADERS,
            timeout=timeout,
            verify=bool_verify,
        )
        resp_req.raise_for_status()
        return resp_req

    def parse_raw_file(
        self,
        resp_req: Union[Response, PlaywrightPage, SeleniumWebDriver],
    ) -> Union[Response, PlaywrightPage, SeleniumWebDriver]:
        """Pass through the response object for HTML parsing in transform_data.

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
        """Parse the HTML table and return a DataFrame.

        Parameters
        ----------
        resp_req : Union[Response, PlaywrightPage, SeleniumWebDriver]
            The response object containing the HTML page.

        Returns
        -------
        pd.DataFrame
            The parsed DataFrame.
        """
        bs_html = HtmlHandler().bs_parser(resp_req)
        bs_table = bs_html.find("table")
        return self._td_th_parser(bs_table)

    def _get_td_th_raw(
        self,
        bs_table: BeautifulSoup,
    ) -> tuple[list[Any], list[Any], list[Any]]:
        """Extract raw header and data cells from an HTML table.

        Parameters
        ----------
        bs_table : BeautifulSoup
            The parsed HTML table element.

        Returns
        -------
        tuple[list[Any], list[Any], list[Any]]
            A tuple of (list_th_raw, list_td_raw, list_outlooks_raw).
        """
        list_outlooks_raw: list[Any] = []
        list_th_raw = [
            th.get_text().replace("\n", "").replace("/", "_").strip().upper()
            for th in bs_table.find_all("th")
        ]
        list_td_raw = [
            float(td.get_text().replace("\n", "").replace("%", "").strip()) / 100.0
            if NumHandler().is_numeric(td.get_text().replace("\n", "").replace("%", "").strip())
            else td.get_text().replace("\n", "").replace("%", "").strip()
            for td in bs_table.find_all("td")
            if len(
                td.get_text().replace("\n", "").replace("%", "").replace("-", "0").strip()
            ) > 0
        ]
        try:
            for div in bs_table.find_all("div", class_="table-normal text-end"):
                try:
                    str_outlook = StrHandler().replace_all(
                        div.find("i")["class"][1],
                        {"fa-circle-down": "DOWNWARD", "fa-circle-up": "UPWARD"},
                    )
                    list_outlooks_raw.append(str_outlook)
                except (AttributeError, TypeError):
                    continue
            int_col_dir = list_th_raw.index("DIRECTION")
            int_rng_upper = len(list_td_raw) // (len(list_th_raw) - 1)
            for i in range(int_rng_upper):
                list_td_raw.insert(3 + (int_col_dir + 3) * i, list_outlooks_raw[i])
        except ValueError:
            pass
        return list_th_raw, list_td_raw, list_outlooks_raw

    def _trt_td_th_raw(
        self,
        list_th_raw: list[Any],
        list_td_raw: list[Any],
    ) -> Union[dict[str, list[Any]], list[dict[str, Any]]]:
        """Build a serializable structure from raw table headers and data cells.

        Parameters
        ----------
        list_th_raw : list[Any]
            Raw table headers.
        list_td_raw : list[Any]
            Raw table data cells.

        Returns
        -------
        Union[dict[str, list[Any]], list[dict[str, Any]]]
            A dict or list-of-dicts ready for DataFrame construction.
        """
        list_th_dts = [x for x in list_th_raw if len(x) > 0]
        list_td_rts_names = [x for x in list_td_raw if isinstance(x, str)]
        list_td_rts_values = [x for x in list_td_raw if NumHandler().is_number(x) is True]
        list_rts_names = self._repeat_elements(list_td_rts_names, len(list_th_dts))
        list_rt_dts = self._repeat_list(list_th_dts, len(list_td_rts_names))
        if self._SOURCE_NAME in {
            "ester", "sonia", "sofr", "usa_cpi", "british_cpi", "canadian_cpi", "european_cpi",
        }:
            list_rt_dts = list_td_rts_names.copy()
            list_rts_names = [self._SOURCE_NAME.upper()] * len(list_td_rts_values)
        list_ser: Any = {
            "DATE": list_rt_dts,
            "RATE_NAME": list_rts_names,
            "RATE_VALUE": list_td_rts_values,
        }
        if self._SOURCE_NAME == "central_banks":
            list_cols = ["CENTRAL_BANK"] + ListHandler().remove_duplicates(list_rt_dts)
            list_data = ListHandler().remove_consecutive_duplicates(list_rts_names)
            for i_rt_val, i in enumerate(
                range(2, len(list_data) + len(list_td_rts_values) + 1, 6)
            ):
                i_rt_val_ = i_rt_val * 2
                list_data.insert(i, list_td_rts_values[i_rt_val_])
                list_data.insert(i + 2, list_td_rts_values[i_rt_val_ + 1])
            list_ser = HandlingDicts().pair_headers_with_data(list_cols, list_data)
        return list_ser

    def _td_th_parser(self, bs_table: BeautifulSoup) -> pd.DataFrame:
        """Parse a BeautifulSoup table element into a DataFrame.

        Parameters
        ----------
        bs_table : BeautifulSoup
            The parsed HTML table element.

        Returns
        -------
        pd.DataFrame
            The parsed DataFrame.
        """
        list_th_raw, list_td_raw, _ = self._get_td_th_raw(bs_table)
        list_ser = self._trt_td_th_raw(list_th_raw, list_td_raw)
        return pd.DataFrame(list_ser)

    @staticmethod
    def _repeat_list(list_: list[Any], n: int) -> list[Any]:
        """Repeat a list n times.

        Parameters
        ----------
        list_ : list[Any]
            The list to repeat.
        n : int
            The number of repetitions.

        Returns
        -------
        list[Any]
            The repeated list.
        """
        return list_ * n

    @staticmethod
    def _repeat_elements(list_: list[Any], n: int) -> list[Any]:
        """Repeat each element in a list n times in sequence.

        Parameters
        ----------
        list_ : list[Any]
            The list whose elements to repeat.
        n : int
            The number of times each element is repeated.

        Returns
        -------
        list[Any]
            The list with each element repeated n times.
        """
        return [element for element in list_ for _ in range(n)]
