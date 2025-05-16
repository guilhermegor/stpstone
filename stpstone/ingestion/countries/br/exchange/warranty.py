import pandas as pd
from typing import Optional
from sqlalchemy.orm import Session
from logging import Logger
from requests import Response
from stpstone._config.global_slots import YAML_B3_UP2DATA_REGISTRIES
from stpstone.utils.connections.netops.proxies.managers.free import YieldFreeProxy
from stpstone.ingestion.abc.requests import ABCRequests


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
    # ! TODO: inner-join bond issuers accepted by b3 with banks participating in brazillian rts
