import re
import pandas as pd
from datetime import datetime
from typing import Optional, List
from sqlalchemy.orm import Session
from logging import Logger
from requests import Response
from time import sleep
from stpstone._config.global_slots import YAML_WW_RATINGS_CORP_S_AND_P
from stpstone.utils.cals.handling_dates import DatesBR
from stpstone.ingestion.abc.requests import ABCRequests
from stpstone.utils.webdriver_tools.selenium_wd import SeleniumWD
from stpstone.utils.loggs.create_logs import CreateLog


class RatingsCorpSPGlobalConcreteCreator(ABCRequests):

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
        regex_pattern = "(?i)system_access_token=([^*]+?);"
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
        regex_match = re.search(
            regex_pattern,
            list_network_traffic[int_idx_bearer]["params"]["headers"]["set-cookie"]
        )
        if (regex_match is not None) \
            and (regex_match.group(0) is not None) \
            and (len(regex_match.group(0)) > 0):
            return f"Bearer {regex_match.group(1)}"

    def req_trt_injection(self, resp_req: Response) -> Optional[pd.DataFrame]:
        return pd.DataFrame(resp_req.json()["RatingAction"])


class RatingsCorpSPGlobalProduct:

    def __init__(
        self,
        session: Optional[Session] = None,
        dt_ref: datetime = DatesBR().sub_working_days(DatesBR().curr_date, 1),
        cls_db: Optional[Session] = None,
        logger: Optional[Logger] = None,
        token: Optional[str] = None,
        list_slugs: Optional[List[str]] = None,
        pg_number: int = 1
    ) -> None:
        self.session = session
        self.dt_ref = dt_ref
        self.cls_db = cls_db
        self.logger = logger
        self.token = token
        self.list_slugs = list_slugs
        self.pg_number = pg_number

    @property
    def get_corp_ratings(self) -> pd.DataFrame:
        list_ser = list()
        str_bearer = RatingsCorpSPGlobalConcreteCreator(bearer=None).get_bearer
        for i in range(1, 100):
            try:
                cls_ = RatingsCorpSPGlobalConcreteCreator(pg_number=i, bearer=str_bearer)
                df_ = cls_.source("ratings_corp", bl_fetch=True)
                list_ser.extend(df_.to_dict(orient="records"))
                sleep(10)
            except Exception as e:
                CreateLog().log_message(self.logger, f"Error: {e}", log_level="warning")
        return pd.DataFrame(list_ser)

    @property
    def update_db(self) -> None:
        str_bearer = RatingsCorpSPGlobalConcreteCreator(bearer=None).get_bearer
        for i in range(1, 100):
            try:
                cls_ = RatingsCorpSPGlobalConcreteCreator(pg_number=i, bearer=str_bearer)
                _ = cls_.source("ratings_corp", bl_fetch=False)
                sleep(10)
            except Exception as e:
                CreateLog().log_message(self.logger, f"Error: {e}", log_level="warning")
