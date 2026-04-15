"""S&P Global corporate ratings orchestrator ingestion."""

from datetime import date
from logging import Logger
from time import sleep
from typing import Optional

import pandas as pd
from requests import Session

from stpstone.ingestion.countries.ww.registries.ratings_corp_spglobal_one_page import (
    RatingsCorpSPGlobalOnePage,
)
from stpstone.transformations.validation.metaclass_type_checker import TypeChecker
from stpstone.utils.loggs.create_logs import CreateLog


class RatingsCorpSPGlobal(metaclass=TypeChecker):
    """Orchestrator that paginates through all S&P Global corporate rating actions."""

    def __init__(
        self,
        date_ref: Optional[date] = None,
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
    ) -> None:
        """Initialize the orchestrator.

        Parameters
        ----------
        date_ref : Optional[date]
            The date of reference forwarded to each page ingestion, by default None.
        logger : Optional[Logger]
            The logger, by default None.
        cls_db : Optional[Session]
            The database session, by default None.

        Returns
        -------
        None
        """
        self.date_ref = date_ref
        self.logger = logger
        self.cls_db = cls_db
        self.cls_create_log = CreateLog()

    @property
    def get_corp_ratings(self) -> pd.DataFrame:
        """Fetch all corporate rating actions across all available pages.

        Returns
        -------
        pd.DataFrame
            Combined DataFrame of all rating actions.
        """
        list_ser: list[dict] = []
        str_bearer = RatingsCorpSPGlobalOnePage(
            bearer="", date_ref=self.date_ref, logger=self.logger
        ).get_bearer
        for i in range(1, 100):
            try:
                cls_ = RatingsCorpSPGlobalOnePage(
                    bearer=str_bearer,
                    pg_number=i,
                    date_ref=self.date_ref,
                    logger=self.logger,
                )
                df_ = cls_.run()
                if df_ is None or df_.empty:
                    break
                list_ser.extend(df_.to_dict(orient="records"))
                sleep(10)
            except Exception as e:
                self.cls_create_log.log_message(
                    self.logger, f"Pagination stopped at page {i}: {e}", log_level="warning"
                )
                break
        return pd.DataFrame(list_ser)

    def update_db(self) -> None:
        """Insert all corporate rating actions into the database, page by page.

        Returns
        -------
        None
        """
        str_bearer = RatingsCorpSPGlobalOnePage(
            bearer="", date_ref=self.date_ref, logger=self.logger
        ).get_bearer
        for i in range(1, 100):
            try:
                cls_ = RatingsCorpSPGlobalOnePage(
                    bearer=str_bearer,
                    pg_number=i,
                    date_ref=self.date_ref,
                    logger=self.logger,
                    cls_db=self.cls_db,
                )
                cls_.run()
                sleep(10)
            except Exception as e:
                self.cls_create_log.log_message(
                    self.logger, f"DB update stopped at page {i}: {e}", log_level="warning"
                )
                break
