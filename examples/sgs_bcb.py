from stpstone.ingestion.countries.br.macroeconomics.sgs_bcb import SGSBCB
from stpstone.utils.calendars.calendar_abc import DatesBR


cls_ = SGSBCB(
    date_start=DatesBR().sub_working_days(DatesBR().curr_date(), 300),
    date_end=DatesBR().sub_working_days(DatesBR().curr_date(), 1)
)
df_ = cls_.source("resource", bool_fetch=True)
print(f"DF SGS BCB: \n{df_}")
df_.info()
