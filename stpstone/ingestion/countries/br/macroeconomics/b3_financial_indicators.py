"""B3 Financial Indicators."""

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


class B3FinancialIndicators(ABCIngestionOperations):
    """Ingestion concrete class."""
    
    def __init__(
        self, 
        date_ref: Optional[date] = None, 
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
    ) -> None:
        """Initialize the ingestion class.
        
        Parameters
        ----------
        date_ref : Optional[date]
            The date of reference, by default None.
        logger : Optional[Logger]
            The logger, by default None.
        cls_db : Optional[Session]
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
        self.url = "https://sistemaswebb3-derivativos.b3.com.br/financialIndicatorsProxy/FinancialIndicators/GetFinancialIndicators/eyJsYW5ndWFnZSI6InB0LWJyIn0=" # noqa E501: line too long
    
    def run(
        self,
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = True,
        bool_insert_or_ignore: bool = False, 
        str_table_name: str = "br_b3_financial_indicators"
    ) -> Optional[pd.DataFrame]:
        """Run the ingestion process.
        
        If the database session is provided, the data is inserted into the database.
        Otherwise, the transformed DataFrame is returned.

        Parameters
        ----------
        timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]]
            The timeout, by default (12.0, 21.0)
        bool_verify : bool
            Whether to verify the SSL certificate, by default True
        bool_insert_or_ignore : bool
            Whether to insert or ignore the data, by default False
        str_table_name : str
            The name of the table, by default "<COUNTRY>_<SOURCE>_<TABLE_NAME>"

        Returns
        -------
        Optional[pd.DataFrame]
            The transformed DataFrame.
        """
        resp_req = self.get_response(timeout=timeout, bool_verify=bool_verify)
        json_ = self.parse_raw_file(resp_req)
        df_ = self.transform_data(json_=json_)
        df_ = self.standardize_dataframe(
            df_=df_, 
            date_ref=self.date_ref,
            dict_dtypes={
                "SECURITY_IDENTIFICATION_CODE": str,
                "DESCRIPTION": str, 
                "GROUP_DESCRIPTION": str, 
                "VALUE": str,
                "RATE": str,
                "LAST_UPDATE": "date",
            }, 
            str_fmt_dt="DD/MM/YYYY",
            cols_from_case="camel",
            cols_to_case="upper_constant",
            url=self.url,
        )
        if self.cls_db:
            self.insert_table_db(
                cls_db=self.cls_db, 
                str_table_name=str_table_name, 
                df_=df_, 
                bool_insert_or_ignore=bool_insert_or_ignore
            )
        else:
            return df_

    @backoff.on_exception(
        backoff.expo, 
        requests.exceptions.HTTPError, 
        max_time=60
    )
    def get_response(
        self, 
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0), 
        bool_verify: bool = True
    ) -> Union[Response, PlaywrightPage, SeleniumWebDriver]:
        """Return a list of response objects.

        Parameters
        ----------
        timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]]
            The timeout, by default (12.0, 21.0)
        bool_verify : bool
            Verify the SSL certificate, by default True
        
        Returns
        -------
        Union[Response, PlaywrightPage, SeleniumWebDriver]
            A list of response objects.
        """
        dict_headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7', # noqa E501: line too long
            'accept-language': 'en-US,en;q=0.9,pt;q=0.8,es;q=0.7',
            'cache-control': 'max-age=0',
            'priority': 'u=0, i',
            'sec-ch-ua': '"Not)A;Brand";v="8", "Chromium";v="138", "Google Chrome";v="138"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Linux"',
            'sec-fetch-dest': 'document',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'none',
            'sec-fetch-user': '?1',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',# noqa E501: line too long
            'Cookie': '_ga=GA1.1.246440908.1756239909; OptanonAlertBoxClosed=2025-09-08T21:47:45.364Z; cf_clearance=81AcleE6TNbW7IQgud9uy_sRa.5sfjueLkqe15gz4nE-1757968582-1.2.1.1-CNPYCUrIpQ34h8MLWylDptCJ3h3I1N5e8_vEUAb26o6j0hHmnpxNGzO.F.w.noAbfnIhjijYSOoNOidWDzJBlRZhfiQU1rWnz1M1Xl0pmgtz21.dD61GyRVv.HG0hUMtmZshO6Y6YlluHo1PDlCp9hw6aMG1vhe4O5IBtctKdde4WL2yi_U.nZ0WziBNeslqLoxRum_KWH5Xv5rHZfI8xoE73VuUeM0NhS4yPREoApI; OptanonConsent=isGpcEnabled=0&datestamp=Mon+Sep+15+2025+17%3A36%3A22+GMT-0300+(Hor%C3%A1rio+Padr%C3%A3o+de+Bras%C3%ADlia)&version=6.21.0&isIABGlobal=false&hosts=&landingPath=NotLandingPage&groups=C0003%3A0%2CC0001%3A1%2CC0004%3A0%2CC0002%3A0&AwaitingReconsent=false&geolocation=%3B; _clck=e74245%5E2%5Efzd%5E0%5E2064; _clsk=1xre1y3%5E1757986628229%5E1%5E1%5Es.clarity.ms%2Fcollect; _ga_SS7FXRTPP3=GS2.1.s1757986670$o22$g0$t1757986670$j60$l0$h0; dtCookie=v_4_srv_24_sn_111A186CD109496F79AB6311DF80D8C5_perc_100000_ol_0_mul_1_app-3Afd69ce40c52bd20e_1_rcs-3Acss_0; TS01f22489=011d592ce13e3b778b79689befe5f561d64aba71e5ed75a3c0d1fd2cce64eb4760e39aae9a66740cf72b8aa5a13be525cd2aa8c0be; F051234a800=!pewsEGmYU9E2E4XjrAp71Xe4MLiPRnYX/lLdLWbPesXD7rTWMil0bAq8t2yZhsUPA51yD5Aw0/1S130=; __cf_bm=LYis5OMEuWIbKLZz6fIFaMNRXkq06HW6Q9kJbdKUa1w-1758057854-1.0.1.1-q87Wdy9R7UJ06pWyEZlX0kVHaU5aDnD8bQO1yI08doDs8P7sJhSKNnK_qtTmlnMI0aqEnxBmDO.WUkxJ.qfJbp6wjv8RNXb.KoFtjsYQrSE' # noqa E501: line too long
        }
        resp_req = requests.get(
            self.url, timeout=timeout, verify=bool_verify, headers=dict_headers)
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
            The parsed content.
        """
        return resp_req.json()
    
    def transform_data(
        self, 
        json_: list[dict[str, Union[str, int, float]]]
    ) -> pd.DataFrame:
        """Transform a list of response objects into a DataFrame.
        
        Parameters
        ----------
        json_ : list[dict[str, Union[str, int, float]]]
            The list of dictionaries to transform.
        
        Returns
        -------
        pd.DataFrame
            The transformed DataFrame.
        """
        return pd.DataFrame(json_)