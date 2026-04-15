"""Implementation of ingestion instance with series names using Playwright."""

from datetime import date
from logging import Logger
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
from stpstone.utils.webdriver_tools.playwright_wd import PlaywrightScraper


class BCBSGS(ABCIngestionOperations):
    """BCB SGS with enhanced series name retrieval using Playwright."""
    
    def __init__(
        self, 
        list_series_codes: Optional[list[int]] = None,
        date_start: Optional[date] = None, 
        date_end: Optional[date] = None,
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
    ) -> None:
        """Initialize the ingestion class.
        
        Parameters
        ----------
        list_series_codes : Optional[list[int]]
            List of series codes to retrieve
        date_start : Optional[date]
            Start date, by default None (30 working days ago)
        date_end : Optional[date]
            End date, by default None (yesterday)
        logger : Optional[Logger]
            The logger, by default None
        cls_db : Optional[Session]
            The database session, by default None
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
        self.list_series_codes = list_series_codes or [11, 4390, 433, 3695, 190]
        self.date_start = date_start or \
            self.cls_dates_br.add_working_days(self.cls_dates_current.curr_date(), -30)
        self.date_end = date_end or \
            self.cls_dates_br.add_working_days(self.cls_dates_current.curr_date(), -1)
        
        self.url_data = "https://api.bcb.gov.br/dados/" \
        + "serie/bcdata.sgs.{}/dados?formato=json&dataInicial={}&dataFinal={}" # codespell:ignore
        self.url_metadata = "https://www3.bcb.gov.br/sgspub/consultarmetadados/consultarMetadadosSeries.do?method=consultarMetadadosSeriesInternet&hdOidSerieSelecionada={}" # noqa E501: line too long
        
        self._series_names_cache = {}
        
        self.cls_scraper = PlaywrightScraper(
            bool_headless=True,
            int_default_timeout=30_000,
            bool_accept_cookies=True,
            logger=self.logger
        )
    
    def run(
        self,
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = True,
        bool_insert_or_ignore: bool = False, 
        str_table_name: str = "br_bcb_sgs"
    ) -> Optional[pd.DataFrame]:
        """Run the ingestion process.
        
        Parameters
        ----------
        timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]]
            The timeout, by default (12.0, 21.0)
        bool_verify : bool
            Whether to verify the SSL certificate, by default True
        bool_insert_or_ignore : bool
            Whether to insert or ignore the data, by default False
        str_table_name : str
            The name of the table, by default "br_bcb_sgs"

        Returns
        -------
        Optional[pd.DataFrame]
            The transformed DataFrame.
        """
        list_ser: list[dict[str, Union[str, int, float]]] = []

        for int_serie_code in self.list_series_codes:
            tuple_metadata = self.get_series_metadata(int_serie_code)
            url = self.url_data.format(
                int_serie_code, 
                self.date_start.strftime("%d/%m/%Y"), 
                self.date_end.strftime("%d/%m/%Y")
            )
            
            resp_req = self.get_response(url=url, timeout=timeout, bool_verify=bool_verify)
            json_ = self.parse_raw_file(resp_req=resp_req)
            df_ = self.transform_data(json_=json_)
            
            df_["CODIGO_SERIE"] = int_serie_code
            df_["NOME_COMPLETO"] = tuple_metadata[0]
            df_["NOME_ABREVIADO"] = tuple_metadata[1]
            df_["PERIODICIDADE"] = tuple_metadata[2]
            df_["UNIDADE"] = tuple_metadata[3]
            df_["DATA_INICIO"] = tuple_metadata[4]
            df_ = self.standardize_dataframe(
                df_=df_, 
                date_ref=self.date_end,
                dict_dtypes={
                    "DATA": "date",
                    "VALOR": float,
                    "CODIGO_SERIE": int,
                    "NOME_COMPLETO": str,
                    "NOME_ABREVIADO": str,
                    "PERIODICIDADE": str,
                    "UNIDADE": str,
                    "DATA_INICIO": str,
                }, 
                str_fmt_dt="DD/MM/YYYY",
                url=url,
                cols_from_case="lower_constant", 
                cols_to_case="upper_constant"
            )
            list_ser.extend(df_.to_dict(orient="records"))
            
        df_final = pd.DataFrame(list_ser)
        
        if self.cls_db:
            self.insert_table_db(
                cls_db=self.cls_db, 
                str_table_name=str_table_name, 
                df_=df_final, 
                bool_insert_or_ignore=bool_insert_or_ignore
            )
        else:
            return df_final
        
    def get_series_metadata(self, series_code: int) -> tuple[str, ...]:
        """Get the series metadata from BCB or fallback mapping.
        
        Parameters
        ----------
        series_code : int
            The series code
            
        Returns
        -------
        tuple[str, ...]
            Tuple with (full_name, short_name, periodicity, unit, start_date)
        """
        if series_code in self._series_names_cache:
            return self._series_names_cache[series_code]
        
        try:
            tuple_metadata = self._fetch_series_name_with_playwright(series_code)
                
            if tuple_metadata:
                self._series_names_cache[series_code] = tuple_metadata
                return tuple_metadata
        except Exception as e:
            self.cls_create_log.log_message(
                self.logger,
                f"Failed to fetch series name for {series_code}: {e}", 
                "error"
            )
        
        tuple_metadata = self._get_series_metadata_from_mapping(series_code)
        self._series_names_cache[series_code] = tuple_metadata
        return tuple_metadata
    
    def _fetch_series_name_with_playwright(self, series_code: int) -> Optional[tuple[str, ...]]:
        """Fetch series name from BCB metadata using Playwright.
        
        Parameters
        ----------
        series_code : int
            The series code
            
        Returns
        -------
        Optional[tuple[str, ...]]
            Tuple with (full_name, short_name, periodicity, unit, start_date) if found
        """
        iframe_selector: str = "iframe[name='iDadosBasicos']"
        url = self.url_metadata.format(series_code)
        
        try:
            with self.cls_scraper.launch():
                if not self.cls_scraper.navigate(url, timeout=30000):
                    self.cls_create_log.log_message(
                        self.logger,
                        f"Failed to navigate to metadata page for series {series_code}",
                        "error"
                    )
                    return None
                
                try:
                    self.cls_scraper.page.wait_for_selector(iframe_selector, timeout=10000)

                    iframe_element = self.cls_scraper.page.locator(iframe_selector)
                    iframe_frame = iframe_element.content_frame()
                    
                    if not iframe_frame:
                        self.cls_create_log.log_message(
                            self.logger,
                            f"Could not access iframe content for series {series_code}",
                            "error"
                        )
                        return None
                    
                    iframe_frame.wait_for_selector("table tbody tr", timeout=10000)
                    
                    try:
                        
                        rows = iframe_frame.locator("table tbody tr").all()
                        
                        full_name = ""
                        short_name = ""
                        periodicity = ""
                        unit = ""
                        start_date = ""
                        for row in rows:
                            cells = row.locator("td").all()
                            if len(cells) >= 4:
                                first_cell_text = cells[0].text_content().strip()
                                second_cell_text = cells[1].text_content().strip()
                                third_cell_text = cells[2].text_content().strip()
                                fourth_cell_text = cells[3].text_content().strip()
                                
                                if "Full name" in first_cell_text:
                                    full_name = second_cell_text
                                    short_name = fourth_cell_text
                                elif "Periodicity" in first_cell_text:
                                    periodicity = fourth_cell_text
                                elif "Unit" in third_cell_text:
                                    unit = third_cell_text
                                elif "Start date" in first_cell_text:
                                    start_date = second_cell_text.strip()
                        
                        if full_name:
                            return (full_name, short_name, periodicity, unit, start_date)
                        else:
                            self.cls_create_log.log_message(
                                self.logger,
                                "Could not extract series name from iframe for "
                                + f"series {series_code}",
                                "warning"
                            )
                            return None
                            
                    except Exception as parse_error:
                        self.cls_create_log.log_message(
                            self.logger,
                            "Error parsing iframe content for "
                            + f"series {series_code}: {parse_error}",
                            "error"
                        )
                        return None
                        
                except Exception as iframe_error:
                    self.cls_create_log.log_message(
                        self.logger,
                        f"Error accessing iframe for series {series_code}: {iframe_error}",
                        "error"
                    )
                    return None
                        
        except Exception as e:
            if self.logger:
                self.cls_create_log.log_message(
                    self.logger,
                    f"Error fetching metadata with Playwright for series {series_code}: {e}",
                    "error"
                )
            
        return None
    
    def _get_series_metadata_from_mapping(self, series_code: int) -> tuple[str, ...]:
        """Get series metadata from predefined mapping.
        
        Parameters
        ----------
        series_code : int
            The series code
            
        Returns
        -------
        tuple[str, ...]
            Tuple with (full_name, short_name, periodicity, unit, start_date)
        """
        series_mapping = {
            11: ("Taxa de juros - Selic", 
                 "Selic", 
                 "Diária", 
                 "% a.d.", 
                 "4/6/1986"),
            4390: ("Taxa de juros - Selic acumulada no mês", 
                   "Selic acumulada no mês", 
                   "Mensal", 
                   "% a.m.", 
                   "31/7/1986"),
            1178: ("Taxa de juros - Selic em termos anuais (base 252)", 
                   "Selic em termos anuais (base 252)", 
                   "Diária", 
                   "% a.a.", 
                   "4/6/1986"),
            4189: ("Taxa de juros - Selic acumulada no mês em termos anuais (base 252)", 
                   "Selic acumulada no mês em termos anuais (base 252)", 
                   "Mensal", 
                   "% a.a.", 
                   "31/7/1986"),
            432: ("Taxa de juros - CDI", 
                  "CDI", 
                  "Diária", 
                  "% a.d.", 
                  "2/1/1986"),
            4391: ("Taxa de juros - CDI acumulada no mês", 
                   "CDI acumulada no mês", 
                   "Mensal", 
                   "% a.m.", 
                   "31/1/1986"),
            1: ("Dólar americano (venda)", 
                "Dólar (venda)", 
                "Diária", 
                "R$/US$", 
                "1/7/1994"),
            3695: ("IPCA - Variação mensal", 
                   "IPCA mensal", 
                   "Mensal", 
                   "% a.m.", 
                   "31/1/1980"),
            433: ("IPCA - Variação acumulada em 12 meses", 
                  "IPCA 12 meses", 
                  "Mensal", 
                  "% a.a.", 
                  "31/12/1979"),
            189: ("IGP-M - Variação mensal", 
                  "IGP-M mensal", 
                  "Mensal", 
                  "% a.m.", 
                  "30/6/1989"),
            190: ("IGP-M - Variação acumulada em 12 meses", 
                  "IGP-M 12 meses", 
                  "Mensal", 
                  "% a.a.", 
                  "31/5/1990"),
        }
        
        return series_mapping.get(
            series_code, 
            (f"SERIE_{series_code}", "", "", "", "")
        )

    @backoff.on_exception(
        backoff.expo, 
        requests.exceptions.HTTPError, 
        max_time=60
    )
    def get_response(
        self, 
        url: str,
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0), 
        bool_verify: bool = True
    ) -> Union[Response, PlaywrightPage, SeleniumWebDriver]:
        """Return a response object.

        Parameters
        ----------
        url : str
            The URL to request
        timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]]
            The timeout, by default (12.0, 21.0)
        bool_verify : bool
            Verify the SSL certificate, by default True
        
        Returns
        -------
        Union[Response, PlaywrightPage, SeleniumWebDriver]
            The response object.
        """
        resp_req = requests.get(url, timeout=timeout, verify=bool_verify)
        resp_req.raise_for_status()
        return resp_req
    
    def parse_raw_file(
        self, 
        resp_req: Union[Response, PlaywrightPage, SeleniumWebDriver]
    ) -> list[dict[str, Union[str, int, float]]]:
        """Parse the raw file content.
        
        Parameters
        ----------
        resp_req : Union[Response, PlaywrightPage, SeleniumWebDriver]
            The response object.
        
        Returns
        -------
        list[dict[str, Union[str, int, float]]]
            The parsed JSON content.
        """
        return resp_req.json()

    def transform_data(
        self, 
        json_: list[dict[str, Union[str, int, float]]]
    ) -> pd.DataFrame:
        """Transform JSON data into a DataFrame.
        
        Parameters
        ----------
        json_ : list[dict[str, Union[str, int, float]]]
            The JSON data.
        
        Returns
        -------
        pd.DataFrame
            The transformed DataFrame.
        """
        return pd.DataFrame(json_)