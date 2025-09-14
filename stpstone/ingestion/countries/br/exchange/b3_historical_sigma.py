"""B3 Historical Sigma Ingestion.

This module provides an ingestion class for B3 Historical Sigma data.
It handles the retrieval of data from the B3 website and stores it in a database.
"""

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
from stpstone.utils.parsers.dicts import HandlingDicts
from stpstone.utils.parsers.folders import DirFilesManagement


class B3HistoricalSigma(ABCIngestionOperations):
    """B3 Historical Sigma Ingestion Class."""
    
    def __init__(
        self, 
        date_ref: Optional[date] = None, 
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
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
        self.cls_dicts_handler = HandlingDicts()
        self.date_ref = date_ref or \
            self.cls_dates_br.add_working_days(self.cls_dates_current.curr_date(), -1)
        self.fstr_url_1 = "https://sistemaswebb3-listados.b3.com.br/securitiesVolatilityProxy/" \
            + "SecuritiesVolatilityCall/GetListVolatilities/" \
            + "eyJsYW5ndWFnZSI6InB0LWJyIiwia2V5d29yZCI6IiIsInBhZ2VOdW1iZXIiOj" \
            + "{}sInBhZ2VTaXplIjo2MH0="
    
    def run(
        self,
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = True,
        bool_insert_or_ignore: bool = False, 
        str_table_name: str = "br_b3_historical_sigma"
    ) -> Optional[pd.DataFrame]:
        """Run the ingestion process.
        
        If the database session is provided, the data is inserted into the database.
        Otherwise, the transformed DataFrame is returned.

        Parameters
        ----------
        timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]], optional
            The timeout, by default (12.0, 21.0)
        bool_verify : bool, optional
            Whether to verify the SSL certificate, by default True
        bool_insert_or_ignore : bool, optional
            Whether to insert or ignore the data, by default False
        str_table_name : str, optional
            The name of the table, by default "br_b3_historical_sigma"

        Returns
        -------
        Optional[pd.DataFrame]
            The transformed DataFrame.
        """
        list_resp_req = self.get_response(timeout=timeout, bool_verify=bool_verify)
        list_ser = self.parse_raw_file(list_resp_req=list_resp_req)
        df_ = self.transform_data(list_ser=list_ser)
        df_ = self.standardize_dataframe(
            df_=df_, 
            date_ref=self.date_ref,
            dict_dtypes={
                "CODE": str,
                "TRADING_NAME": str,
                "SERIE": str, # codespell:ignore
                "STANDARD_DEVIATION_1": float,
                "ANNUALIZED_VOLATILITY_1": float,
                "STANDARD_DEVIATION_3": float,
                "ANNUALIZED_VOLATILITY_3": float,
                "STANDARD_DEVIATION_6": float,
                "ANNUALIZED_VOLATILITY_6": float,
                "STANDARD_DEVIATION_12": float,
                "ANNUALIZED_VOLATILITY_12": float,
                "PAGE_NUMBER": int,
                "PAGE_SIZE": int,
                "TOTAL_RECORDS": int,
                "TOTAL_PAGES": int,
                "URL": str,
            }, 
            str_fmt_dt="YYYY-MM-DD",
            cols_from_case="camel",
            cols_to_case="upper_constant",
            url=None
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

    def get_response(
        self, 
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0), 
        bool_verify: bool = True
    ) -> Union[list[Response], list[PlaywrightPage], list[SeleniumWebDriver]]:
        """Return a list of response objects.

        Parameters
        ----------
        timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]], optional
            The timeout, by default (12.0, 21.0)
        bool_verify : bool, optional
            Verify the SSL certificate, by default True
        
        Returns
        -------
        Union[list[Response], list[PlaywrightPage], list[SeleniumWebDriver]]
            A list of response objects.
        """
        list_: list[str] = []
        
        list_.extend(self.get_individual_response(
            fstr_url=self.fstr_url_1,
            list_apps=["E", "I", "M", "Q", "U", "Y", "c", "g"],
            timeout=timeout,
            bool_verify=bool_verify
        ))

        return list_

    @backoff.on_exception(
        backoff.expo, 
        requests.exceptions.HTTPError, 
        max_time=60
    )
    def get_individual_response(
        self, 
        fstr_url: str,
        list_apps: list[str],
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0), 
        bool_verify: bool = True
    ) -> Union[list[Response], list[PlaywrightPage], list[SeleniumWebDriver]]:
        """Return a list of response objects.
        
        Parameters
        ----------
        fstr_url : str
            The URL.
        list_apps : list[str]
            The list of apps.
        timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]], optional
            The timeout, by default (12.0, 21.0)
        bool_verify : bool, optional
            Verify the SSL certificate, by default True
        
        Returns
        -------
        Union[list[Response], list[PlaywrightPage], list[SeleniumWebDriver]]
            A list of response objects.
        """
        list_: list[str] = []

        for app in list_apps:
            resp_req = requests.get(
                fstr_url.format(app), 
                timeout=timeout, 
                verify=bool_verify, 
                headers={
                    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7', # noqa E501: line too long
                    'accept-language': 'en-US,en;q=0.9,pt;q=0.8,es;q=0.7',
                    'cache-control': 'max-age=0',
                    'priority': 'u=0, i',
                    'sec-ch-ua': '"Not)A;Brand";v="8", "Chromium";v="138", "Google Chrome";v="138"', # noqa E501: line too long
                    'sec-ch-ua-mobile': '?0',
                    'sec-ch-ua-platform': '"Linux"',
                    'sec-fetch-dest': 'document',
                    'sec-fetch-mode': 'navigate',
                    'sec-fetch-site': 'none',
                    'sec-fetch-user': '?1',
                    'upgrade-insecure-requests': '1',
                    'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36', # noqa E501: line too long
                    'Cookie': 'cf_clearance=itszyrYiXr_toGtzXiIcCSCG7x0lWwCn69TAg9ouAGE-1756239908-1.2.1.1-YB4wmQcLRebNZeEEvA2cM_wvG.MoMGdf2COeGIdPAv.tFZKcvCXnMPMmZEQGDyyPmmhcF788EVzWmpt3mfdPS3DoxgFONQLHrBXdW74Hlwa.eAQcdz9uSMMgLUEf5nXnaxQ8OpAtQfeoWYZA4c0Zmz2C5cN4upPHH7a2TzeOOv45MMqDzUbxoEmdmX4iWkPEyuuhZWuRi0qmg157frqsBiwmuMdDU.THOSj0SaOyd9Q; _clck=e74245%5E2%5Efys%5E0%5E2064; _ga=GA1.1.246440908.1756239909; OptanonConsent=isGpcEnabled=0&datestamp=Tue+Aug+26+2025+17%3A25%3A13+GMT-0300+(Hor%C3%A1rio+Padr%C3%A3o+de+Bras%C3%ADlia)&version=6.21.0&isIABGlobal=false&hosts=&landingPath=NotLandingPage&groups=C0003%3A0%2CC0001%3A1%2CC0004%3A0%2CC0002%3A0&AwaitingReconsent=false; _ga_SS7FXRTPP3=GS2.1.s1756239908$o1$g1$t1756239939$j29$l0$h0; __cf_bm=ADLsMTtgNWeE1hxV328TmSmmL5T0h5juIl9HDTeB5QI-1757366809-1.0.1.1-7h3DELukUG6euRfReS72LRMnTrimUT16GDO2wtnV8FTwTAdzqx8SWbe1DnLMBhdXpgNSSQDII.bmd1Xvhsx8sNdVHnqJuOVrXu.JNayywFE; dtCookie=v_4_srv_28_sn_10CF716BB87F454582E5FEB0008052DD_perc_100000_ol_0_mul_1_app-3Afd69ce40c52bd20e_0; TS01f22489=011d592ce18e9ac1ca3a299cef05b705e6afd43fc37d490e9f885f490d69ee6bea3a26badfa2073de71994d26aa346e63fcd013653' # noqa E501: line too long
                }
            )
            resp_req.raise_for_status()
            list_.append(resp_req)

        return list_
    
    def parse_raw_file(
        self, 
        list_resp_req: Union[list[Response], list[PlaywrightPage], list[SeleniumWebDriver]]
    ) -> list[dict[str, Union[str, int, float]]]:
        """Parse the raw file content.
        
        Parameters
        ----------
        list_resp_req : Union[list[Response], list[PlaywrightPage], list[SeleniumWebDriver]]
            The response object.
        
        Returns
        -------
        list[dict[str, Union[str, int, float]]]
            The parsed content.
        """
        list_ser: list[dict[str, Union[str, int, float]]] = []

        for resp_req in list_resp_req:
            json_ = resp_req.json()
            list_ = self.cls_dicts_handler.add_key_value_to_dicts(
                list_ser=json_["results"], 
                key=[json_["page"]]
            )
            list_ = self.cls_dicts_handler.add_key_value_to_dicts(
                list_ser=list_, 
                key="URL", 
                value=resp_req.url
            )
            list_ser.extend(list_)
        
        return list_ser
    
    def transform_data(
        self, 
        list_ser: list[dict[str, Union[str, int, float]]]
    ) -> pd.DataFrame:
        """Transform a list of response objects into a DataFrame.
        
        Parameters
        ----------
        list_ser : list[dict[str, Union[str, int, float]]]
            The list of dictionaries to transform.
        
        Returns
        -------
        pd.DataFrame
            The transformed DataFrame.
        """
        df_ = pd.DataFrame(list_ser)
        df_ = df_.drop_duplicates()
        return df_