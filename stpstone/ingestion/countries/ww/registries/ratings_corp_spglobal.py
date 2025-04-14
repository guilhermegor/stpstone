import re
import pandas as pd
from datetime import datetime
from typing import Optional, List
from sqlalchemy.orm import Session
from logging import Logger
from requests import Response
from time import sleep
from pprint import pprint
from stpstone.utils.parsers.json import JsonFiles
from stpstone._config.global_slots import YAML_WW_RATINGS_CORP_S_AND_P
from stpstone.utils.cals.handling_dates import DatesBR
from stpstone.ingestion.abc.requests import ABCRequests
from stpstone.utils.webdriver_tools.selenium_wd import SeleniumWD


class RatingsCorpSPGlobal(ABCRequests):

    def __init__(
        self,
        bearer: str,
        session: Optional[Session] = None,
        dt_ref: datetime = DatesBR().sub_working_days(DatesBR().curr_date, 1),
        cls_db: Optional[Session] = None,
        logger: Optional[Logger] = None,
        token: Optional[str] = None,
        list_slugs: Optional[List[str]] = None,
        pg_number: int = 1
    ) -> None:
        super().__init__(
            dict_metadata=YAML_WW_RATINGS_CORP_S_AND_P,
            session=session,
            dt_ref=dt_ref,
            cls_db=cls_db,
            logger=logger,
            token=token,
            list_slugs=list_slugs
        )
        self.bearer = bearer
        self.session = session
        self.dt_ref = dt_ref
        self.cls_db = cls_db
        self.logger = logger
        self.list_slugs = list_slugs
        self.pg_number = pg_number

    @property
    def get_bearer(self) -> str:
        pattern_regex = "(?i)system_access_token=([^*]+?);"
        url = "https://disclosure.spglobal.com/ratings/en/regulatory/ratings-actions"
        cls_selenium = SeleniumWD(url, bl_headless=True, bl_incognito=True)
        cls_selenium.wait(60)
        cls_selenium.wait_until_el_loaded('//a[@class="link-black link-black-hover text-underline"]')
        sleep(60)
        list_network_traffic = cls_selenium.get_network_traffic
        for i, dict_ in enumerate(list_network_traffic):
            if dict_["method"] == "Network.responseReceivedExtraInfo":
                int_idx_bearer = i
                break
        # JsonFiles().dump_message(list_network_traffic, r"C:\Users\guiro\Downloads\network-traffic-spglobal-disclosure_20250414_0743.json")
        # print(f"pattern regex: {pattern_regex}")
        # print(f"set cookie: {list_network_traffic[int_idx_bearer]['params']['headers']['set-cookie']}")
        regex_match = re.search(
            pattern_regex,
            list_network_traffic[int_idx_bearer]["params"]["headers"]["set-cookie"]
        )
        # print(f"regex match: {regex_match}")
        if (regex_match is not None) \
            and (regex_match.group(0) is not None) \
            and (len(regex_match.group(0)) > 0):
            # print(f"Bearer {regex_match.group(1)}")
            return f"Bearer {regex_match.group(1)}"

    def req_trt_injection(self, req_resp: Response) -> Optional[pd.DataFrame]:
        return pd.DataFrame(req_resp.json()["RatingAction"])
