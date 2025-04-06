# pypi.org libs
import os
# local libs
os.path.abspath(os.path.join(os.path.realpath(__file__), '..'))
from stpstone.utils.connections.netops.proxies.proxy_scrape import ProxyScrapeAll
from stpstone.utils.cals.handling_dates import DatesBR
from stpstone.ingestion.tradings.ww.exchange.markets.yf_ws import YFinanceProvider


list_tickers = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'TSLA']
# session = ProxyScrapeAll(bl_new_proxy=True, bl_use_timer=True).session
session = None

if session is not None:
    cls_yf = YFinanceProvider(
        list_tickers=list_tickers,
        dt_inf=DatesBR().sub_working_days(DatesBR().curr_date, 52),
        dt_sup=DatesBR().sub_working_days(DatesBR().curr_date, 1),
        session=session
    )
else:
    cls_yf = YFinanceProvider(
        list_tickers=list_tickers,
        dt_inf=DatesBR().sub_working_days(DatesBR().curr_date, 52),
        dt_sup=DatesBR().sub_working_days(DatesBR().curr_date, 1)
    )

print(cls_yf.mktdata)
