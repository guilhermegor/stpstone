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
import pandas as pd
from datetime import datetime
from typing import Optional, List, Any, Tuple
from sqlalchemy.orm import Session
from logging import Logger
from requests import Response
from time import sleep
from stpstone._config.global_slots import YAML_EXAMPLE
from stpstone.utils.cals.handling_dates import DatesBR
from stpstone.ingestion.abc.requests import ABCRequests
from stpstone.utils.webdriver_tools.playwright_wd import PlaywrightScraper


class ConcreteCreatorReq(ABCRequests):

    def __init__(
        self,
        session: Optional[Session] = None,
        dt_ref: datetime = DatesBR().sub_working_days(DatesBR().curr_date, 1),
        cls_db: Optional[Session] = None,
        logger: Optional[Logger] = None,
        token: Optional[str] = None,
        list_slugs: Optional[List[str]] = None, 
        int_wait_load_seconds: int = 60, 
        int_delay_seconds: int = 30,
        bl_save_html: bool = False, 
        bl_headless: bool = False,
        bl_incognito: bool = False, 
    ) -> None:
        super().__init__(
            dict_metadata=YAML_EXAMPLE,
            session=session,
            dt_ref=dt_ref,
            cls_db=cls_db,
            logger=logger,
            token=token,
            list_slugs=list_slugs,
            int_wait_load_seconds=int_wait_load_seconds, 
            int_delay_seconds=int_delay_seconds,
        )
        self.session = session
        self.dt_ref = dt_ref
        self.cls_db = cls_db
        self.logger = logger
        self.list_slugs = list_slugs
        self.int_wait_load_seconds = int_wait_load_seconds
        self.int_delay_seconds = int_delay_seconds
        self.bl_save_html = bl_save_html
        self.bl_headless = bl_headless
        self.bl_incognito = bl_incognito

    def td_iterative(self, i: int, scraper: PlaywrightScraper) -> Dict[str, Any]:
        pass

    def td_generic(self, scraper: PlaywrightScraper) -> List[Dict[str, Any]]:
        pass

    def req_trt_injection(self, resp_req: Response) -> Optional[pd.DataFrame]:
        i = 1
        list_ser = list()
        source = self.get_query_params(resp_req.url, "source")
        scraper = PlaywrightScraper(
            bl_headless=self.bl_headless,
            int_default_timeout=self.int_wait_load_seconds * 1_000, 
            bl_incognito=self.bl_incognito
        )
        with scraper.launch():
            if scraper.navigate(resp_req.url):
                if self.bl_save_html:
                    scraper.export_html(
                        scraper.page.content(), 
                        folder_path="data", 
                        filename="html-mais-retorno-avl-funds", 
                        bl_include_timestamp=True
                    )
                if source == "iterative":
                    while True:
                        if not scraper.selector_exists(
                            YAML_EXAMPLE[source]["xpaths"]["p_example"].format(i),
                            selector_type="xpath",
                            timeout=self.int_wait_load_seconds * 1_000
                        ): break
                        list_ser.append(self.td_iterative(i, scraper))
                        i += 1
                elif source == "generic":
                    list_ser = td_generic(scraper)
        return pd.DataFrame(list_ser)

EOF

echo "File succesfully created at: $full_dir_path/$file_name.py"
