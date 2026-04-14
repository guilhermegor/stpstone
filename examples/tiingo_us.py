# pypi.org libs
import os

from keyring import get_password


# local libs
os.path.abspath(os.path.join(os.path.realpath(__file__), ".."))
from stpstone.ingestion.countries.us.exchange.tiingo import TiingoUS
from stpstone.utils.calendars.calendar_br import DatesBRAnbima


df_ = TiingoUS(
	session=None,
	date_start=DatesBRAnbima().sub_working_days(DatesBRAnbima().curr_date(), 52),
	date_end=DatesBRAnbima().sub_working_days(DatesBRAnbima().curr_date(), 1),
	cls_db=None,
	token=get_password("TIINGO", "API_KEY"),
	list_slugs=["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "TSLA", "META"],
).source("ohlcv_adjusted", bool_debug=False, bool_fetch=True)
print(f"DF TIINGO: \n{df_}")
