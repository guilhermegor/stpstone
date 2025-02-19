# pypi.org libs
import os
# local libs
os.path.abspath(os.path.join(os.path.realpath(__file__), '..'))
from stpstone.utils.connections.netops.session import ReqSession
from stpstone.utils.cals.handling_dates import DatesBR
from stpstone.utils.exchange.ww.yf_provider import YFinanceProvider


list_tickers = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'TSLA']
session = ReqSession(bl_new_proxy=True, bl_use_timer=True).session

cls_yf = YFinanceProvider(
    list_tickers=list_tickers, 
    dt_beg=DatesBR().sub_working_days(DatesBR().curr_date, 52), 
    dt_end=DatesBR().sub_working_days(DatesBR().curr_date, 1),
    session=session
)

print(cls_yf.mktdata)