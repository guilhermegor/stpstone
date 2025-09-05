#!/bin/bash

# define the project root directory
PROJECT_ROOT="$(pwd)/stpstone"

# prompt for folder path within the project
read -p "Enter the PY folder path within the project (default: ./ingestion): " folder_path
folder_path=${folder_path:-./ingestion}

# ensure the folder path is within the project directory
if [[ "$folder_path" != ./* ]]; then
    echo "Error: The folder path must be within the project directory."
    exit 1
fi

# construct the full directory path
full_dir_path="$PROJECT_ROOT/$folder_path"

# ensure the directory exists
mkdir -p "$full_dir_path"

# require path and file name
read -p "Enter the PY file name (without extension, default: request_config): " file_name
file_name=${file_name:-request_config}

# create yaml file
cat <<EOF > "$full_dir_path/$file_name.py"
"""Implementation of ingestion instance."""

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
from stpstone.utils.loggs.create_logs import CreateLog
from stpstone.utils.parsers.folders import DirFilesManagement


class IngestionConcreteClass(ABCIngestionOperations):
    """Ingestion concrete class."""
    
    def __init__(
        self, 
        list_apps: list[str], 
        int_pages_join: Optional[int] = 3,  
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
    ) -> None:
        """Initialize the ingestion class.
        
        Parameters
        ----------
        list_apps : list[str]
            The list of apps.
        int_pages_join : Optional[int], optional
            The number of pages to join, by default 3.
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
        
        self.list_apps = list_apps
        self.int_pages_join = int_pages_join
        self.logger = logger
        self.cls_db = cls_db
        self.cls_dir_files_management = DirFilesManagement()
        self.cls_dates_current = DatesCurrent()
        self.cls_create_log = CreateLog()

    @backoff.on_exception(
        backoff.expo, 
        requests.exceptions.HTTPError, 
        max_time=60
    )
    def get_response(
        self, 
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0)
    ) -> Union[Response, PlaywrightPage, SeleniumWebDriver]:
        """Return a list of response objects.

        Parameters
        ----------
        timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]], optional
            The timeout, by default (12.0, 21.0)
        
        Returns
        -------
        list[requests.Response]
            A list of response objects.
        """
        pass
    
    def transform_response(
        self, 
        resp_req: Union[Response, PlaywrightPage, SeleniumWebDriver]
    ) -> pd.DataFrame:
        """Transform a list of response objects into a DataFrame.
        
        Parameters
        ----------
        resp_req: Union[Response, PlaywrightPage, SeleniumWebDriver]
            The response object.
        
        Returns
        -------
        pd.DataFrame
            The transformed DataFrame.
        """
        pass
    
    def run(self) -> Optional[pd.DataFrame]:
        """Run the ingestion process.
        
        If the database session is provided, the data is inserted into the database.
        Otherwise, the transformed DataFrame is returned.

        Returns
        -------
        Optional[pd.DataFrame]
            The transformed DataFrame.
        """
        resp_req = self.get_response()
        df_ = self.transform_response(resp_req)
        df_ = self.standardize_dataframe(
            df_=df_, 
            date_ref=self.cls_dates_current.curr_date(),
            dict_dtypes={
                "COL_1": str,
                "COL_2": str, 
                "COL_3": str
            }
        )
        if self.cls_db:
            self.insert_table_db(
                cls_db=self.cls_db, 
                str_table_name="<COUNTRY_ORIGIN_NAME>", 
                df_=df_
            )
        else:
            return df_
EOF

echo "File succesfully created at: $full_dir_path/$file_name.py"
