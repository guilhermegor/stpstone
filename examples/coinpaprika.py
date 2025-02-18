# pypi.org libs
import os
# local libs
os.path.abspath(os.path.join(os.path.realpath(__file__), '..'))
from stpstone.ingestion.tradings.ww.exchange.crypto.coinpaprika import CoinPaprika
from stpstone.utils.cals.handling_dates import DatesBR
from stpstone.utils.connections.netops.session import ReqSession


session = ReqSession(bl_new_proxy=True).session
df_ = CoinPaprika(
    session=session,
    dt_ref=DatesBR().sub_working_days(DatesBR().curr_date, 5), 
    cls_db=None
)._source('ohlcv_latest', bl_debug=True, bl_fetch=True)
print(f'DF MKTDATA COIN PAPRIKA: \n{df_}')