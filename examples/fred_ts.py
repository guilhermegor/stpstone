import dotenv

from stpstone.ingestion.countries.us.macroeconomics.fred import FredUSMacro
from stpstone.utils.calendars.calendar_br import DatesBRAnbima


fred_api_key = dotenv.get_key(".env", "FRED_KEY")
fred_ts = FredUSMacro(
	date_start=DatesBRAnbima().sub_working_days(DatesBRAnbima().curr_date(), 10),
	date_end=DatesBRAnbima().sub_working_days(DatesBRAnbima().curr_date(), 1),
	api_key=fred_api_key,
)
df_ = fred_ts.source("resource", bool_fetch=True)
print(f"DF FRED TS: \n{df_}")
