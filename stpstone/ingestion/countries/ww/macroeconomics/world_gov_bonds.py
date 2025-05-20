import pandas as pd
from datetime import datetime
from typing import Optional, List
from sqlalchemy.orm import Session
from logging import Logger
from requests import Response
from time import sleep
from stpstone._config.global_slots import YAML_WW_WORLD_GOV_BONDS, YAML_WW_RATINGS_AGENCIES
from stpstone.utils.cals.handling_dates import DatesBR
from stpstone.ingestion.abc.requests import ABCRequests
from stpstone.utils.webdriver_tools.playwright_wd import PlaywrightScraper
from stpstone.utils.parsers.dicts import HandlingDicts
from stpstone.utils.parsers.lists import ListHandler
from stpstone.utils.parsers.numbers import NumHandler


class WorldGovBonds(ABCRequests):

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
            dict_metadata=YAML_WW_WORLD_GOV_BONDS,
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

    def treat_list_td(self, list_td: List[str]) -> List[str]:
        """
        Processes the list of table data cells to insert "N/A" for countries without ratings.
        
        The rule is: After a country name, if the next item is a numeric value (like '9.88%' or '0.0988'),
        insert "N/A" between them to represent missing rating.
        
        Args:
            list_td: List of strings representing table data cells
            
        Returns:
            Processed list with "N/A" inserted where needed
        """
        list_ratings_agencies = ListHandler().extend_lists(
            *YAML_WW_RATINGS_AGENCIES["credit_ratings"].values())
        list_ = []
        i = 0
        n = len(list_td)
        while i < n:
            item_curr = list_td[i]
            item_processed = NumHandler().transform_to_float(list_td[i], int_precision=6)
            list_.append(item_processed)
            bl_country_name = (
                isinstance(item_curr, str) and
                not any(c.isdigit() for c in item_curr) and 
                not item_curr.endswith('%') and 
                item_curr != "N/A" and 
                item_curr not in list_ratings_agencies
            )
            if bl_country_name:
                if i + 1 < n:
                    next_item = list_td[i+1]
                    bl_next_numeric = (
                        (isinstance(next_item, str) and next_item.endswith('%')) 
                        or (isinstance(NumHandler().transform_to_float(next_item, int_precision=6), 
                                       (int, float)))
                    )
                    if bl_next_numeric:
                        list_.append("N/A")
            i += 1
        return list_

    def req_trt_injection(self, resp_req: Response) -> Optional[pd.DataFrame]:
        source = self.get_query_params(resp_req.url, "source")
        list_th = list(YAML_WW_WORLD_GOV_BONDS[source]["dtypes"].keys())
        scraper = PlaywrightScraper(
            bl_headless=True,
            int_default_timeout=100_000
        )
        with scraper.launch():
            if scraper.navigate(resp_req.url):
                list_td = scraper.get_list_data(
                    YAML_WW_WORLD_GOV_BONDS[source]["xpaths"]["list_td"],
                    selector_type="xpath"
                )
        list_td = self.treat_list_td(list_td)
        if source == "sovereign_cds": list_td = list_td[:-1]
        list_ser = HandlingDicts().pair_headers_with_data(list_th, list_td)
        return pd.DataFrame(list_ser)