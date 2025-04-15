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
from stpstone.utils.parsers.html import HtmlHandler
from stpstone.utils.parsers.folders import DirFilesManagement
from stpstone.utils.parsers.dicts import HandlingDicts
from stpstone.utils.parsers.str import StrHandler
from stpstone.utils.loggs.create_logs import CreateLog


class ConcreteCreatorReq(ABCRequests):

    def __init__(
        self,
        session: Optional[Session] = None,
        dt_ref: datetime = DatesBR().sub_working_days(DatesBR().curr_date, 1),
        cls_db: Optional[Session] = None,
        logger: Optional[Logger] = None,
        token: Optional[str] = None,
        list_slugs: Optional[List[str]] = None
    ) -> None:
        super().__init__(
            dict_metadata=YAML_EXAMPLE,
            session=session,
            dt_ref=dt_ref,
            cls_db=cls_db,
            logger=logger,
            token=token,
            list_slugs=list_slugs
        )
        self.session = session
        self.dt_ref = dt_ref
        self.cls_db = cls_db
        self.logger = logger
        self.token = token
        self.list_slugs = list_slugs

    def td_th_parser(self, req_resp: Response, list_th: List[Any]) -> Tuple[List[Any], int, Optional[int]]:
        list_headers = list_th.copy()
        int_init_td = 0
        int_end_td = None
        # for using this workaround, please pass a dummy variable to the url, within the YAML file,
        #   like https://example.com/app/#source=dummy_1&bl_debug=True
        if StrHandler().match_string_like(req_resp.url, "*#source=dummy_1*") == True:
            list_headers = [
                list_th[0],
                list_th[...]
            ]
            int_init_td = 0
            int_end_td = 200
        elif StrHandler().match_string_like(req_resp.url, "*#source=dummy_2*") == True:
            list_headers = [
                list_th[0],
                list_th[...]
            ]
            int_init_td = 200
            int_end_td = None
        else:
            if self.logger is not None:
                CreateLog().warning(
                    self.logger,
                    "No source found in url, for HTML webscraping, please revisit the code"
                    + f" if it is an unexpected behaviour - URL: {req_resp.url}"
                )
        return list_headers, int_init_td, int_end_td

    def req_trt_injection(self, req_resp: Response) -> Optional[pd.DataFrame]:
        bl_debug = True if StrHandler().match_string_like(
            req_resp.url, "*bl_debug=True*") == True else False
        root = HtmlHandler().lxml_parser(req_resp)
        # export html tree to data folder, if is user's will
        if bl_debug == True:
            path_project = DirFilesManagement().find_project_root(marker="pyproject.toml")
            HtmlHandler().html_tree(root, file_path=rf"{path_project}\data\test.html")
        list_th = [
            x.text.strip() for x in HtmlHandler().lxml_xpath(
                root, YAML_EXAMPLE["source"]["xpaths"]["list_th"]
            )
        ]
        list_td = [
            "" if x.text is None else x.text.replace("\xa0", "").strip()
            for x in HtmlHandler().lxml_xpath(
                root, YAML_EXAMPLE["source"]["xpaths"]["list_td"]
            )
        ]
        # deal with data/headers specificity for the project
        list_headers, int_init_td, int_end_td = self.td_th_parser(req_resp, list_th)
        if bl_debug == True:
            print(list_headers)
            print(f"LEN LIST HEADERS: {len(list_headers)}")
            print(list_td[int_init_td:int_end_td])
            print(f"LEN LIST TD: {len(list_td[int_init_td:int_end_td])}")
        list_ser = HandlingDicts().pair_headers_with_data(
            list_headers,
            list_td[int_init_td:int_end_td]
        )
        return pd.DataFrame(list_ser)

EOF

echo "File succesfully created at: $full_dir_path/$file_name.py"
