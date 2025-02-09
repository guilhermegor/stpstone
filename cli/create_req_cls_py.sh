#!/bin/bash

# require path and file name
read -p "Digite o caminho onde deseja criar o arquivo: " file_path
read -p "Digite o nome do arquivo Python (sem .py): " file_name

# ensuring the path exists
mkdir -p "$file_path"

full_path="$file_path/$file_name.py"

cat > "$full_path" <<EOL
### MASK REQUEST ###

# pypi.org libs
import pandas as pd
from datetime import datetime
from typing import Optional
from sqlalchemy.orm import Session, Dict
from logging import Logger
from requests import Response
# project modules
from stpstone._config._global_slots import YAML_EXAMPLE
from stpstone.utils.cals.handling_dates import DatesBR
from stpstone.connections.netops.session import ReqSession
from stpstone.ingestion.abc.requests import ABCRequests

class MSKRequest(ABCRequests):

    def __init__(
        self,
        session: Optional[ReqSession] = None,
        dt_ref: datetime = DatesBR().sub_working_days(DatesBR().curr_date, 1),
        cls_db: Optional[Session] = None,
        logger: Optional[Logger] = None
    ) -> None:
        self.token = self.access_token
        super().__init__(
            dict_metadata=YAML_EXAMPLE,
            session=session,
            dt_ref=dt_ref,
            cls_db=cls_db,
            logger=logger
        )
    
    def trt_injection(self, req_resp: Response) -> Optional[pd.DataFrame]:
        return None
EOL

echo "Arquivo criado com sucesso em: $full_path"
