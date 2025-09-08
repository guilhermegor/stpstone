#!/bin/bash

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
MAGENTA='\033[0;35m'
NC='\033[0m'

PROJECT_ROOT="$(pwd)/stpstone"

print_status() {
    local status="$1"
    local message="$2"
    case "$status" in
        "success") echo -e "${GREEN}[✓]${NC} ${message}" ;;
        "error") echo -e "${RED}[✗]${NC} ${message}" >&2 ;;
        "warning") echo -e "${YELLOW}[!]${NC} ${message}" ;;
        "info") echo -e "${BLUE}[i]${NC} ${message}" ;;
        "config") echo -e "${CYAN}[→]${NC} ${message}" ;;
        "debug") echo -e "${MAGENTA}[»]${NC} ${message}" ;;
        *) echo -e "[ ] ${message}" ;;
    esac
}

validate_folder_path() {
    local folder_path="$1"
    
    if [[ "$folder_path" != ./* ]]; then
        print_status "error" "The folder path must be within the project directory."
        return 1
    fi
    
    return 0
}

create_directory() {
    local full_dir_path="$1"
    
    mkdir -p "$full_dir_path"
    if [ $? -ne 0 ]; then
        print_status "error" "Failed to create directory: $full_dir_path"
        return 1
    fi
    
    print_status "success" "Directory created: $full_dir_path"
    return 0
}

create_python_file() {
    local full_dir_path="$1"
    local file_name="$2"
    
    cat <<EOF > "$full_dir_path/$file_name.py"
"""Implementation of ingestion instance."""

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


class IngestionConcreteClass(ABCIngestionOperations):
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
        self.url = "FILL_ME"

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
        timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]], optional
            The timeout, by default (12.0, 21.0)
        bool_verify : bool, optional
            Verify the SSL certificate, by default True
        
        Returns
        -------
        Union[Response, PlaywrightPage, SeleniumWebDriver]
            A list of response objects.
        """
        resp_req = requests.get(self.url, timeout=timeout, verify=bool_verify)
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
        return self.get_file(resp_req=resp_req)
    
    def transform_data(
        self, 
        file: StringIO
    ) -> pd.DataFrame:
        """Transform a list of response objects into a DataFrame.
        
        Parameters
        ----------
        file : StringIO
            The parsed content.
        
        Returns
        -------
        pd.DataFrame
            The transformed DataFrame.
        """
        return pd.read_csv(file, sep=";")
    
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
        bool_verify : bool, optional
            Whether to verify the SSL certificate, by default True
        bool_insert_or_ignore : bool, optional
            Whether to insert or ignore the data, by default False
        str_table_name : str, optional
            The name of the table, by default "<COUNTRY>_<SOURCE>_<TABLE_NAME>"

        Returns
        -------
        Optional[pd.DataFrame]
            The transformed DataFrame.
        """
        resp_req = self.get_response(timeout=timeout, bool_verify=bool_verify)
        file = self.parse_raw_file(resp_req)
        df_ = self.transform_data(file=file)
        df_ = self.standardize_dataframe(
            df_=df_, 
            date_ref=self.date_ref,
            dict_dtypes={
                "COL_1": str,
                "COL_2": float, 
                "COL_3": int, 
                "COL_4": "date"
            }, 
            str_fmt_dt="YYYY-MM-DD",
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
EOF

    if [ $? -eq 0 ]; then
        print_status "success" "Python file created: $full_dir_path/$file_name.py"
        return 0
    else
        print_status "error" "Failed to create Python file: $full_dir_path/$file_name.py"
        return 1
    fi
}

create_test_file() {
    local test_dir_path="$1"
    local file_name="$2"
    
    mkdir -p "$test_dir_path"
    if [ $? -ne 0 ]; then
        print_status "error" "Failed to create test directory: $test_dir_path"
        return 1
    fi
    
    local test_file_path="$test_dir_path/test_$file_name.py"
    touch "$test_file_path"
    
    if [ $? -eq 0 ]; then
        print_status "success" "Test file created: $test_file_path"
        return 0
    else
        print_status "error" "Failed to create test file: $test_file_path"
        return 1
    fi
}

create_example_file() {
    local example_dir_path="$1"
    local file_name="$2"
    
    mkdir -p "$test_dir_path"
    if [ $? -ne 0 ]; then
        print_status "error" "Failed to create example directory: $example_dir_path"
        return 1
    fi
    
    local example_file_path="$example_dir_path/$file_name.py"
    touch "$example_file_path"
    
    if [ $? -eq 0 ]; then
        print_status "success" "Test file created: $example_file_path"
        return 0
    else
        print_status "error" "Failed to create test file: $example_file_path"
        return 1
    fi
}

main() {
    print_status "info" "Starting Python file creation script"
    
    read -p "Enter the PY folder path within the project (default: ./ingestion): " folder_path
    folder_path=${folder_path:-./ingestion}
    
    validate_folder_path "$folder_path" || exit 1
    
    full_dir_path="$PROJECT_ROOT/$folder_path"
    
    create_directory "$full_dir_path" || exit 1
    
    read -p "Enter the PY file name (without extension, default: request_config): " file_name
    file_name=${file_name:-request_config}
    
    create_python_file "$full_dir_path" "$file_name" || exit 1
    
    test_dir_path="$PROJECT_ROOT/tests/unit"
    example_dir_path="$PROJECT_ROOT/examples"
    create_test_file "$test_dir_path" "$file_name" || exit 1
    create_example_file "$example_dir_path" "$file_name" || exit 1
    
    print_status "success" "All files created successfully!"
    print_status "info" "Main file: $full_dir_path/$file_name.py"
    print_status "info" "Test file: $test_dir_path/test_$file_name.py"
}

main "$@"