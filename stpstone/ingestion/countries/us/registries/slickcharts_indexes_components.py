"""SlickCharts index components ingestion for S&P 500, NASDAQ 100, and Dow Jones."""

from datetime import date
from logging import Logger
from typing import Optional, Union

import backoff
from lxml.html import HtmlElement
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
from stpstone.utils.parsers.html import HtmlHandler


_BASE_URL = "https://www.slickcharts.com/"

_SOURCE_CONFIG: dict[str, dict[str, str]] = {
    "sp500": {
        "url": f"{_BASE_URL}sp500",
        "xpath_list_tr": (
            "//table[@class=\"table table-hover table-borderless table-sm\"]"
            "/tbody//tr[{}]"
        ),
        "table_name": "us_slickcharts_sp500",
    },
    "nasdaq100": {
        "url": f"{_BASE_URL}nasdaq100",
        "xpath_list_tr": (
            "//table[@class=\"table table-hover table-borderless table-sm\"]"
            "/tbody//tr[contains(@id, \"td-chart\")][{}]"
        ),
        "table_name": "us_slickcharts_nasdaq100",
    },
    "dowjones": {
        "url": f"{_BASE_URL}dowjones",
        "xpath_list_tr": (
            "//table[@class=\"table table-hover table-borderless table-sm\"]"
            "/tbody//tr[{}]"
        ),
        "table_name": "us_slickcharts_dowjones",
    },
}

_ALL_SOURCES: list[str] = list(_SOURCE_CONFIG.keys())


class SlickChartsIndexesComponents(ABCIngestionOperations):
    """SlickCharts index components ingestion class.

    Fetches constituent data (rank, company name, ticker, weight) for major US
    equity indexes (S&P 500, NASDAQ 100, Dow Jones) from slickcharts.com.
    """

    def __init__(
        self,
        date_ref: Optional[date] = None,
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
        list_slugs: Optional[list[str]] = None,
    ) -> None:
        """Initialize the ingestion class.

        Parameters
        ----------
        date_ref : Optional[date], optional
            The date of reference, by default None.
        logger : Optional[Logger], optional
            The logger, by default None.
        cls_db : Optional[Session], optional
            The database session, by default None.
        list_slugs : Optional[list[str]], optional
            The list of index slugs to scrape. Valid values are 'sp500',
            'nasdaq100', 'dowjones'. Defaults to all three when None.

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
        self.list_slugs = list_slugs if list_slugs is not None else _ALL_SOURCES
        self._current_slug: str = self.list_slugs[0]
        self._current_url: str = _SOURCE_CONFIG[self._current_slug]["url"]

    def run(
        self,
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = False,
        bool_insert_or_ignore: bool = False,
        str_table_name: Optional[str] = None,
    ) -> Optional[pd.DataFrame]:
        """Run the ingestion process for all configured index slugs.

        Iterates over each slug in ``list_slugs``, fetches and transforms
        the constituent data, then either inserts into the database or
        concatenates results into a single DataFrame.

        Parameters
        ----------
        timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]], optional
            The request timeout, by default (12.0, 21.0).
        bool_verify : bool, optional
            Whether to verify the SSL certificate, by default False.
        bool_insert_or_ignore : bool, optional
            Whether to insert or ignore duplicate rows, by default False.
        str_table_name : Optional[str], optional
            Override the table name for DB insertion. When None the per-source
            table name from ``_SOURCE_CONFIG`` is used, by default None.

        Returns
        -------
        Optional[pd.DataFrame]
            Concatenated DataFrame for all slugs when no ``cls_db`` is
            provided; None when data is written to the database.
        """
        list_df: list[pd.DataFrame] = []

        for slug in self.list_slugs:
            cfg = _SOURCE_CONFIG[slug]
            self._current_slug = slug
            self._current_url = cfg["url"]

            resp_req = self.get_response(timeout=timeout, bool_verify=bool_verify)
            root = self.parse_raw_file(resp_req)
            df_ = self.transform_data(root=root)
            df_ = self.standardize_dataframe(
                df_=df_,
                date_ref=self.date_ref,
                dict_dtypes={
                    "NUM_COMPANY": int,
                    "NAME_COMPANY": str,
                    "TICKER": str,
                    "WEIGHT": float,
                },
                str_fmt_dt="YYYY-MM-DD",
                url=self._current_url,
            )

            if self.cls_db:
                table = str_table_name or cfg["table_name"]
                self.insert_table_db(
                    cls_db=self.cls_db,
                    str_table_name=table,
                    df_=df_,
                    bool_insert_or_ignore=bool_insert_or_ignore,
                )
            else:
                list_df.append(df_)

        if not self.cls_db:
            return pd.concat(list_df, ignore_index=True) if list_df else pd.DataFrame()
        return None

    @backoff.on_exception(backoff.expo, requests.exceptions.HTTPError, max_time=60)
    def get_response(
        self,
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = False,
    ) -> Union[Response, PlaywrightPage, SeleniumWebDriver]:
        """Fetch the raw HTTP response for the current index slug.

        Parameters
        ----------
        timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]], optional
            The request timeout, by default (12.0, 21.0).
        bool_verify : bool, optional
            Verify the SSL certificate, by default False.

        Returns
        -------
        Union[Response, PlaywrightPage, SeleniumWebDriver]
            The HTTP response object.
        """
        resp_req = requests.get(self._current_url, timeout=timeout, verify=bool_verify)
        resp_req.raise_for_status()
        return resp_req

    def parse_raw_file(
        self,
        resp_req: Union[Response, PlaywrightPage, SeleniumWebDriver],
    ) -> HtmlElement:
        """Parse the HTTP response into an lxml element tree.

        Parameters
        ----------
        resp_req : Union[Response, PlaywrightPage, SeleniumWebDriver]
            The response object returned by ``get_response``.

        Returns
        -------
        HtmlElement
            The root lxml HTML element for XPath querying.
        """
        return HtmlHandler().lxml_parser(resp_req)

    def transform_data(self, root: HtmlElement) -> pd.DataFrame:  # type: ignore[override]
        """Transform the lxml element tree into a DataFrame.

        Parameters
        ----------
        root : HtmlElement
            The root lxml HTML element as returned by ``parse_raw_file``.

        Returns
        -------
        pd.DataFrame
            DataFrame with columns NUM_COMPANY, NAME_COMPANY, TICKER, WEIGHT.
        """
        return self._td_th_parser(root)

    def _td_th_parser(self, root: HtmlElement) -> pd.DataFrame:
        """Extract table rows from the index constituent HTML table.

        Iterates over numbered ``<tr>`` elements using the XPath template
        for the current slug until no more rows are found.

        Parameters
        ----------
        root : HtmlElement
            The root lxml HTML element of the page.

        Returns
        -------
        pd.DataFrame
            DataFrame with columns NUM_COMPANY, NAME_COMPANY, TICKER, WEIGHT.
        """
        xpath_template = _SOURCE_CONFIG[self._current_slug]["xpath_list_tr"]
        cls_html = HtmlHandler()
        list_ser: list[dict[str, object]] = []

        for i in range(1, 600):
            xpath = xpath_template.format(i)
            matches = cls_html.lxml_xpath(root, xpath)
            if not matches:
                break
            el_tr = matches[0]
            list_ser.append({
                "NUM_COMPANY": cls_html.lxml_xpath(el_tr, "./td[1]")[0].text.strip(),
                "NAME_COMPANY": cls_html.lxml_xpath(el_tr, "./td[2]/a")[0].text.strip(),
                "TICKER": cls_html.lxml_xpath(el_tr, "./td[3]/a")[0].text.strip(),
                "WEIGHT": float(
                    cls_html.lxml_xpath(el_tr, "./td[4]")[0].text.strip().replace("%", "")
                ) / 100.0,
            })

        return pd.DataFrame(list_ser)
