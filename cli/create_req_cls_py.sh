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
### SCAFFOLDING INGESTION REQUEST ###

# pypi.org libs
import pandas as pd
from datetime import datetime
from typing import Optional, List
from sqlalchemy.orm import Session
from logging import Logger
from requests import Response
from time import sleep
# project modules
from stpstone._config.global_slots import YAML_EXAMPLE
from stpstone.utils.cals.handling_dates import DatesBR
from stpstone.utils.connections.netops.session import ReqSession
from stpstone.ingestion.abc.requests import ABCRequests


class ScaffoldingReq(ABCRequests):

    def __init__(
        self,
        session:Optional[ReqSession]=None,
        dt_ref:datetime=DatesBR().sub_working_days(DatesBR().curr_date, 1),
        cls_db:Optional[Session]=None,
        logger:Optional[Logger]=None,
        token:Optional[str]=None,
        list_slugs:Optional[List[str]]=None
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

    def req_trt_injection(self, req_resp:Response) -> Optional[pd.DataFrame]:
        return None
EOF

echo "File succesfully created at: $full_dir_path/$file_name.py"
