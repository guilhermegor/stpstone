import dotenv
from stpstone.ingestion.countries.us.macroeconomics.fred import FredUSMacro
from stpstone.utils.cals.handling_dates import DatesBR


fred_api_key = dotenv.get_key(".env", "FRED_KEY")
fred_ts = FredUSMacro(
    dt_start=DatesBR().sub_working_days(DatesBR().curr_date, 10),
    dt_end=DatesBR().sub_working_days(DatesBR().curr_date, 1),
    api_key=fred_api_key
)
df_ = fred_ts.source("resource", bl_fetch=True)
print(f"DF FRED TS: \n{df_}")