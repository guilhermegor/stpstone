### METADATA: https://ranaroussi.github.io/yfinance/index.html ###


# pypi.org libs
import pandas as pd
from yfinance import download
from typing import List, Optional
from datetime import datetime
# local libs
from stpstone.transformations.validation.metaclass_type_checker import TypeChecker
from stpstone.utils.connections.netops.proxies.managers.free import YieldFreeProxy
from stpstone.utils.cals.handling_dates import DatesBR


class YFinanceWS(metaclass=TypeChecker):

    def __init__(
        self,
        list_tickers:List[str],
        dt_start:datetime=DatesBR().sub_working_days(DatesBR().curr_date, 52).strftime('%Y-%m-%d'),
        dt_end:datetime=DatesBR().sub_working_days(DatesBR().curr_date, 1).strftime('%Y-%m-%d'),
        session:Optional[ProxyScrapeAll]=None
    ) -> None:
        self.list_tickers = list_tickers
        self.session = session
        self.dt_start = dt_start
        self.dt_end = dt_end

    @property
    def mktdata(self) -> pd.DataFrame:
        df_ = download(
            tickers=self.list_tickers,
            start=self.dt_start.strftime('%Y-%m-%d'),
            end=self.dt_end.strftime('%Y-%m-%d'),
            group_by='ticker',
            session=self.session
        )
        return df_
