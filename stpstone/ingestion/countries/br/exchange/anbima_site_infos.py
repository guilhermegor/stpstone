import re
import pandas as pd
from datetime import datetime
from typing import Optional, List, Any, Union
from sqlalchemy.orm import Session
from logging import Logger
from requests import Response
from time import sleep
from stpstone._config.global_slots import YAML_ANBIMA_INFOS
from stpstone.utils.cals.handling_dates import DatesBR
from stpstone.ingestion.abc.requests import ABCRequests
from stpstone.utils.parsers.str import StrHandler


class AnbimaInfos(ABCRequests):

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
            dict_metadata=YAML_ANBIMA_INFOS,
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
        self.list_slugs = list_slugs
        self.dt_ref_yymmdd = dt_ref.strftime('%y%m%d')

    def list_rows_injection(self, resp_req: Response) -> Union[List[Any], List[Any]]:
        list_rows_1 = []
        list_rows_2 = []
        raw_text = re.sub(r'--(?=\d)', '--@', resp_req.text)
        raw_text = raw_text.replace(",", ".").replace("--", "-99999").replace("\r", "")
        list_rows_raw = raw_text.split("\n")
        for i, row in enumerate(list_rows_raw):
            if row.startswith("0@") or row.startswith("1@TOTAIS") or row.startswith("2@COMPOSI") \
                or row.startswith("1@Data de Refer") or row.startswith("2@Data de Refer") \
                or len(row) <= 1:
                continue
            elif (row.startswith("1@")) \
                and (not row.startswith("1@TOTAIS")) \
                and (not row.startswith("1@Data de Refer")):
                list_rows_1.append(row[2:])
            elif (row.startswith("2@")) \
                and (not row.startswith("2@COMPOSI")) \
                and (not row.startswith("2@Data de Refer")):
                list_rows_2.append(row[2:])
            elif any(row.startswith(f"{n}@") for n in range(3, 10)):
                raise ValueError(f"ROW #{i}: Unexpected row prefix found: {row}")
            elif "@" not in row:
                raise ValueError(f"ROW #{i}: Unexpected row format found: {row}")
            else:
                raise ValueError(f"ROW #{i}: Unexpected row format found: {row}")
        return list_rows_1, list_rows_2

    def req_trt_injection(self, resp_req: Response) -> Optional[pd.DataFrame]:
        if StrHandler().match_string_like(resp_req.url, '*#source=ima_p2_pvs*'):
            source = "ima_p2_pvs"
            int_idx_row = 1
        elif StrHandler().match_string_like(resp_req.url, '*#source=ima_p2_th_portf*'):
            source = "ima_p2_th_portf"
            int_idx_row = 2
        else:
            source = None
            int_idx_row = None
        if source is None or int_idx_row is None:
            return None
        else:
            list_rows_1, list_rows_2 = self.list_rows_injection(resp_req)
            data_rows = locals()[f"list_rows_{int_idx_row}"]
            df_ = pd.DataFrame([row.split("@") for row in data_rows],
                            columns=list(YAML_ANBIMA_INFOS[source]["dtypes"].keys()))
        return df_
