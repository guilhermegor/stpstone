"""IBGE SIDRA.

This module provides a class for ingesting data from the IBGE SIDRA website.

Notes
-----
[1] Metadata: https://servicodados.ibge.gov.br/api/docs/
"""

from datetime import date
from io import StringIO
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
from stpstone.utils.parsers.html import HtmlHandler


class IBGESIDRA(ABCIngestionOperations):
    """Ingestion concrete class."""
    
    def __init__(
        self, 
        list_series_id: Optional[list[int]] = None,
        date_ref: Optional[date] = None, 
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
    ) -> None:
        """Initialize the ingestion class.
        
        Parameters
        ----------
        list_series_id : Optional[list[int]]
            The list of series IDs, by default None.
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

        if list_series_id is not None:
            self._validate_series_id_input(list_series_id)

        self.list_series_id = list_series_id or [1737, 3065]
        self.logger = logger
        self.cls_db = cls_db
        self.cls_dir_files_management = DirFilesManagement()
        self.cls_dates_current = DatesCurrent()
        self.cls_create_log = CreateLog()
        self.cls_dates_br = DatesBRAnbima()
        self.cls_html_handler = HtmlHandler()
        self.date_ref = date_ref or \
            self.cls_dates_br.add_working_days(self.cls_dates_current.curr_date(), -1)
        self.url_data_service = "https://servicodados.ibge.gov.br/api/v3/agregados/{}/periodos#source=sidra_modification_dates"
    
    def _validate_series_id_input(self, list_series_id: list[int]) -> None:
        """Validate the series id.
        
        Parameters
        ----------
        list_series_id : list[int]
            The list of series ids
        
        Returns
        -------
        None

        Raises
        ------
        TypeError
            If list_series_id is not of type list[int]
            If list_series_id is empty
            If any item in list_series_id is not of type int
        """
        if list_series_id is not None:
            if not isinstance(list_series_id, list):
                raise TypeError("list_series_id must be of type list[int], "
                                f"got {type(list_series_id)}")
            
            if not list_series_id:
                raise TypeError("list_series_id must be of type list[int] and cannot be empty")
            
            for item in list_series_id:
                if not isinstance(item, int) or isinstance(item, bool):
                    raise TypeError("All items in list_series_id must be of "
                                    f"type int, got {type(item)}")
                if item <= 0:
                    raise TypeError("All items in list_series_id must be positive "
                                    f"integers, got {item}")

    def run(
        self,
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = True,
        bool_insert_or_ignore: bool = False, 
        str_table_name: str = "<COUNTRY>_<SOURCE>_<TABLE_NAME>"
    ) -> Optional[pd.DataFrame]:
        """Run the ingestion process.
        
        If the database session is provided, the data is inserted into the database.
        Otherwise, the transformed DataFrame is returned.

        Parameters
        ----------
        timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]], optional
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
        list_ser: list[dict[str, Union[str, int, float]]] = []
        url: str = ""

        for int_series_id in self.list_series_id:
            url = self.url_data_service.format(str(int_series_id))
            resp_req = self.get_response(
                url=url, 
                timeout=timeout, 
                bool_verify=bool_verify
            )
            json_ = self.parse_raw_file(resp_req)
            df_ = self.transform_data(json_=json_)

            tup_metadata = self.get_metadata(int_series_id=int_series_id)
            df_["NAME"] = tup_metadata[0]
            df_["SOURCE"] = tup_metadata[1]
            df_["NOTES"] = tup_metadata[2]
            df_["SERIES_ID"] = int_series_id

            list_ser.extend(df_.to_dict("records"))

        df_ = pd.DataFrame(list_ser)
        if not df_.empty:
            df_ = self.standardize_dataframe(
                df_=df_, 
                date_ref=self.date_ref,
                dict_dtypes={
                    "REFERENCED_PERIOD": str,
                    "MODIFICATION_DATE": "date",
                    "NAME": str,
                    "SOURCE": str,
                    "NOTES": str,
                    "SERIES_ID": int,
                }, 
                str_fmt_dt="DD/MM/YYYY",
                url=url or "No URL available",
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
        url: str,
        dict_headers: Optional[dict[str, str]] = None,
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0), 
        bool_verify: bool = True
    ) -> Union[Response, PlaywrightPage, SeleniumWebDriver]:
        """Return a list of response objects.

        Parameters
        ----------
        url : str
            The URL to request
        dict_headers : Optional[dict[str, str]], optional
            The headers, by default None
        timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]], optional
            The timeout, by default (12.0, 21.0)
        bool_verify : bool
            Verify the SSL certificate, by default True
        
        Returns
        -------
        Union[Response, PlaywrightPage, SeleniumWebDriver]
            A list of response objects.
        """
        resp_req = requests.get(url, timeout=timeout, verify=bool_verify, headers=dict_headers)
        resp_req.raise_for_status()
        return resp_req
    
    def parse_raw_file(
        self, 
        resp_req: Union[Response, PlaywrightPage, SeleniumWebDriver]
    ) -> StringIO:
        """Parse the raw file content.
        
        Parameters
        ----------
        resp_req : Union[Response, PlaywrightPage, SeleniumWebDriver]
            The response object.
        
        Returns
        -------
        StringIO
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
            The parsed content.
        
        Returns
        -------
        pd.DataFrame
            The transformed DataFrame.
        """
        list_referenced_period = [dict_["literals"][1] for dict_ in json_]
        list_modification_dates = [dict_["modificacao"] for dict_ in json_]

        return pd.DataFrame(
            {
                "REFERENCED_PERIOD": list_referenced_period, 
                "MODIFICATION_DATE": list_modification_dates
            }
        )
    
    def get_metadata(
        self, 
        int_series_id: int
    ) -> tuple[str, str, str]:
        """Get the metadata for a given series ID.
        
        Parameters
        ----------
        int_series_id : int
            The series ID.
        
        Returns
        -------
        tuple[str, str, str]
            Table name, source, and description.
        """
        self._validate_int_series_id(int_series_id=int_series_id)
        url = f"https://sidra.ibge.gov.br/Ajax/JSon/Tabela/1/{str(int_series_id)}?versao=-1&_=1758398026370"
        dict_headers = {
            'Accept': '*/*',
            'Accept-Language': 'en-US,en;q=0.9,pt;q=0.8,es;q=0.7',
            'Connection': 'keep-alive',
            'Referer': f'https://sidra.ibge.gov.br/tabela/{str(int_series_id)}',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36', # noqa E501: line too long
            'X-Requested-With': 'XMLHttpRequest',
            'sec-ch-ua': '"Not)A;Brand";v="8", "Chromium";v="138", "Google Chrome";v="138"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Linux"',
            'Cookie': '_gid=GA1.3.1907461132.1758367586; _clck=e1jrcg%5E2%5Efzh%5E0%5E2089; _ga=GA1.1.449810855.1758367586; _ga_0VE4HSDTTT=GS2.1.s1758367586$o1$g1$t1758368844$j47$l0$h0; _clsk=kdtofp%5E1758373548264%5E2%5E1%5Ee.clarity.ms%2Fcollect; _ga_YR1L80XXVD=GS2.1.s1758398009$o1$g1$t1758398026$j43$l0$h0' # noqa E501: line too long
        }

        resp_req = self.get_response(url=url, dict_headers=dict_headers)
        json_ = self.parse_raw_file(resp_req)
        return json_["Nome"], json_["Fonte"], ";".join(
            json_["Notas"] if isinstance(json_["Notas"], list) \
                else [json_["Notas"]] if json_["Notas"] else ""
        )

    def _validate_int_series_id(self, int_series_id: int) -> None:
        """Validate the series ID.
        
        Parameters
        ----------
        int_series_id : int
            The series ID.
        
        Returns
        -------
        None

        Raises
        ------
        TypeError
            If the series ID is not of type int.
            If the series ID is not a positive integer.
        """
        if not isinstance(int_series_id, int) or isinstance(int_series_id, bool):
            raise TypeError(f"int_series_id must be of type int, got {type(int_series_id)}")
        
        if int_series_id <= 0:
            raise TypeError(f"int_series_id must be a positive integer, got {int_series_id}")