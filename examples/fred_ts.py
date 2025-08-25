import dotenv

from stpstone.ingestion.countries.us.macroeconomics.fred import FredUSMacro
from stpstone.utils.cals.cal_abc import DatesBR


fred_api_key = dotenv.get_key(".env", "FRED_KEY")
fred_ts = FredUSMacro(
    date_start=DatesBR().sub_working_days(DatesBR().curr_date(), 10),
    date_end=DatesBR().sub_working_days(DatesBR().curr_date(), 1),
    api_key=fred_api_key
)
df_ = fred_ts.source("resource", bool_fetch=True)
print(f"DF FRED TS: \n{df_}")
