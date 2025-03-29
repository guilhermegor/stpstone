# pypi.org libs
import os
# local libs
os.path.abspath(os.path.join(os.path.realpath(__file__), '..'))
from stpstone.ingestion.tradings.ww.exchange.markets.investingcom import InvestingCom
from stpstone.utils.cals.handling_dates import DatesBR
from stpstone.utils.connections.netops.sessions.proxy_scrape import ReqSession


session = ReqSession(
    bl_new_proxy=True,
    bl_use_timer=True,
    float_min_ratio_times_alive_dead=0.02,
    float_max_timeout=600
).session
print(session.proxies)

cls_investingcom = InvestingCom(
    session=session,
    dt_inf=DatesBR().sub_working_days(DatesBR().curr_date, 5),
    dt_sup=DatesBR().sub_working_days(DatesBR().curr_date, 0),
    str_ticker='PETR4'
)
df_ = cls_investingcom.source('daily_ohlcv', bl_fetch=True, bl_debug=False)
print(f'DF OHLCV: \n{df_}')
print(df_.info())
