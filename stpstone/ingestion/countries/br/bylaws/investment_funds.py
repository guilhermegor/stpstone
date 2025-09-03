"""Bylaws of investment funds from brazilian SEC (CVM).

The CVM (Comissão Valores Mobiliários) is the Brazilian Securities Exchange Commission, 
which is responsible for regulating the securities market in Brazil.

The CVM has a website where you can find information about investment funds, such as their name, 
employer identification number (EIN/CNPJ), and other relevant details.
"""

from io import BytesIO
from typing import Optional

import pandas as pd
import requests
from requests import Session

from stpstone.ingestion.abc.ingestion_abc import ABCIngestionOperations
from stpstone.utils.calendars.calendar_abc import DatesCurrent
from stpstone.utils.parsers.folders import DirFilesManagement


class InvestmentFunds(ABCIngestionOperations):
    
    def __init__(
        self, 
        list_apps: list[str], 
        int_pages_join: Optional[int] = 3, 
        cls_db: Optional[Session] = None
    ) -> None:
        """Initialize the InvestmentFunds class.
        
        Parameters
        ----------
        list_apps : list[str]
            The list of apps.
        int_pages_join : Optional[int], optional
            The number of pages to join, by default 3.
        cls_db : Optional[Session], optional
            The database session, by default None.
        
        Returns
        -------
        None
        """
        self.list_apps = list_apps
        self.int_pages_join = int_pages_join
        self.cls_db = cls_db
        self.cls_dir_files_management = DirFilesManagement()
        self.cls_dates_current = DatesCurrent()

    def get_response(self) -> list[requests.Response]:
        """Return a list of response objects.
        
        Returns
        -------
        list[requests.Response]
            A list of response objects.
        """
        fstr_url = r"https://web.cvm.gov.br/app/fundosweb/fundos/regulamento/obter/por/arquivo/{}"
        list_resp_req = list()
        for app in self.list_apps:
            resp_req = requests.get(fstr_url.format(app))
            resp_req.raise_for_status()
            list_resp_req.append(resp_req)
        return list_resp_req
    
    def transform_response(self, list_resp_req: list[requests.Response]) -> pd.DataFrame:
        """Transform a list of response objects into a DataFrame.
        
        Parameters
        ----------
        list_resp_req : list[requests.Response]
            The response object.
        
        Returns
        -------
        pd.DataFrame
            The transformed DataFrame.
        """
        list_ser = list()

        for resp_req in list_resp_req:
            bytes_file = BytesIO(resp_req.content)
            df_ = self.pdf_docx_tables_response(
                bytes_file=bytes_file, 
                str_file_extension=self.cls_dir_files_management.get_last_file_extension(
                    file_path=resp_req.url
                ), 
                int_pages_join=self.int_pages_join
            )
            df_["URL"] = resp_req.url
            list_ser.extend(df_.to_dict(orient="records"))

        return pd.DataFrame(list_ser)
    
    def run(self) -> Optional[pd.DataFrame]:
        """Run the ingestion process.
        
        If the database session is provided, the data is inserted into the database.
        Otherwise, the transformed DataFrame is returned.

        Returns
        -------
        Optional[pd.DataFrame]
            The transformed DataFrame.
        """
        list_resp_req = self.get_response()
        df_ = self.transform_response(list_resp_req)
        df_ = self.standardize_dataframe(
            df_=df_, 
            date_ref=self.cls_dates_current.curr_date(),
            dict_dtypes={
                "EVENT": str, 
                "MATCH_PATTERN": str, 
                "PATTERN_REGEX": str, 
                "URL": str
            }
        )
        if self.cls_db:
            self.insert_table_db(
                cls_db=self.cls_db, 
                str_table_name="investment_funds", 
                df_=df_
            )
        else:
            return df_