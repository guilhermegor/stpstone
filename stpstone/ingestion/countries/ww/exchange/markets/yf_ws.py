### METADATA: https://ranaroussi.github.io/yfinance/index.html ###


# pypi.org libs
import pandas as pd
from yfinance import download
from typing import List, Optional
from datetime import datetime
# local libs
from stpstone.transformations.validation.metaclass_type_checker import TypeChecker
from stpstone.utils.connections.netops.proxies.proxy_scrape import ProxyScrapeAll
from stpstone.utils.cals.handling_dates import DatesBR


class YFinanceWS(metaclass=TypeChecker):

    def __init__(
        self,
        list_tickers:List[str],
        dt_inf:datetime=DatesBR().sub_working_days(DatesBR().curr_date, 52).strftime('%Y-%m-%d'),
        dt_sup:datetime=DatesBR().sub_working_days(DatesBR().curr_date, 1).strftime('%Y-%m-%d'),
        session:Optional[ProxyScrapeAll]=None
    ) -> None:
        self.list_tickers = list_tickers
        self.session = session
        self.dt_inf = dt_inf
        self.dt_sup = dt_sup

    @property
    def mktdata(self) -> pd.DataFrame:
        df_ = download(
            tickers=self.list_tickers,
            start=self.dt_inf.strftime('%Y-%m-%d'),
            end=self.dt_sup.strftime('%Y-%m-%d'),
            group_by='ticker',
            session=self.session
        )
        return df_
