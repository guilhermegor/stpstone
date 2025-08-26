### METADATA: https://ranaroussi.github.io/yfinance/index.html ###


# pypi.org libs
from datetime import datetime
from typing import List, Optional

import pandas as pd
from yfinance import download

# local libs
from stpstone.transformations.validation.metaclass_type_checker import TypeChecker
from stpstone.utils.calendars.calendar_br import DatesBRAnbima
from stpstone.utils.connections.netops.proxies.managers.free_proxies_manager import YieldFreeProxy


class YFinanceWS(metaclass=TypeChecker):

    def __init__(
        self,
        list_tickers:List[str],
        date_start:datetime=DatesBRAnbima().sub_working_days(DatesBRAnbima().curr_date(), 52).strftime('%Y-%m-%d'),
        date_end:datetime=DatesBRAnbima().sub_working_days(DatesBRAnbima().curr_date(), 1).strftime('%Y-%m-%d'),
        session:Optional[ProxyScrapeAll]=None
    ) -> None:
        self.list_tickers = list_tickers
        self.session = session
        self.date_start = date_start
        self.date_end = date_end

    @property
    def mktdata(self) -> pd.DataFrame:
        df_ = download(
            tickers=self.list_tickers,
            start=self.date_start.strftime('%Y-%m-%d'),
            end=self.date_end.strftime('%Y-%m-%d'),
            group_by='ticker',
            session=self.session
        )
        return df_
