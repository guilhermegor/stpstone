# pypi.org libs
import os
from keyring import get_password
# local libs
os.path.abspath(os.path.join(os.path.realpath(__file__), '..'))
from stpstone.ingestion.tradings.ww.exchange.crypto.coincap import CoinCap
from stpstone.utils.cals.handling_dates import DatesBR
from stpstone.utils.connections.netops.session import ReqSession


session = ReqSession(bl_new_proxy=True).session
df_ = CoinCap(
    session=session,
    dt_ref=DatesBR().sub_working_days(DatesBR().curr_date, 5),
    cls_db=None,
    token=get_password('COIN_CAP', 'API_KEY')
).source('ohlcv_latest', bl_debug=False, bl_fetch=True)
print(f'DF MKTDATA COINCAP: \n{df_}')
