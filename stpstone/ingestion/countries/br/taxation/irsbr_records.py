import pandas as pd
from datetime import datetime
from typing import Optional
from sqlalchemy.orm import Session
from logging import Logger
from requests import Response
from stpstone._config.global_slots import YAML_IRSBR
from stpstone.utils.cals.handling_dates import DatesBR
from stpstone.ingestion.abc.requests import ABCRequests


class IRSBR(ABCRequests):

    def __init__(
        self,
        bl_create_session:bool=False,
        bl_new_proxy:bool=False,
        dt_ref:datetime=DatesBR().sub_working_days(DatesBR().curr_date, 1),
        session: Optional[Session] = None,
        cls_db:Optional[Session]=None,
        logger:Optional[Logger]=None
    ) -> None:
        super().__init__(
            dict_metadata=YAML_IRSBR,
            session=session,
            dt_ref=dt_ref,
            dict_headers={
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                'Accept-Language': 'pt-BR,pt;q=0.9',
                'Connection': 'keep-alive',
                'Referer': 'https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/2025-01/',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'same-origin',
                'Sec-Fetch-User': '?1',
                'Upgrade-Insecure-Requests': '1',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36',
                'sec-ch-ua': '"Not A(Brand";v="8", "Chromium";v="132", "Google Chrome";v="132"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"Windows"'
            },
            dict_payload=None,
            cls_db=cls_db,
            logger=logger,
            year_dt_ref = DatesBR().year_number(self.dt_ref),
            month_dt_ref = DatesBR().month_number(self.dt_ref, bl_month_mm=True)
        )

    def req_trt_injection(self, resp_req:Response) -> Optional[pd.DataFrame]:
        return None
