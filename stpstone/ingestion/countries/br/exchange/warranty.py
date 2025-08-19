from logging import Logger
from typing import Optional

import pandas as pd
from requests import Response
from sqlalchemy.orm import Session

from stpstone._config.global_slots import YAML_B3_UP2DATA_REGISTRIES
from stpstone.ingestion.abc.requests import ABCRequests
from stpstone.utils.connections.netops.proxies.managers.free_proxies_manager import YieldFreeProxy


class BondIssuersWB3(ABCRequests):

    def __init__(
        self,
        session: Optional[Session] = None,
        cls_db:Optional[Session]=None,
        logger:Optional[Logger]=None
    ) -> None:
        self.token = self.access_token
        super().__init__(
            dict_metadata=YAML_B3_UP2DATA_REGISTRIES,
            session=session,
            dict_headers=None,
            dict_payload=None,
            cls_db=cls_db,
            logger=logger
        )

    def req_trt_injection(self, resp_req:Response) -> Optional[pd.DataFrame]:
        return None

    # ! TODO: downstream processing to standardize issuer name in both banks_rts_br and b3_bond_issuers_accp_warranty
    # ! TODO: inner-join bond issuers accepted by b3 with banks participating in brazilian rts
