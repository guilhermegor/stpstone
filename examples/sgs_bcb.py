from stpstone.ingestion.countries.br.macroeconomics.sgs_bcb import SGSBCB
from stpstone.utils.calendars.calendar_br import DatesBRAnbima


cls_ = SGSBCB(
    date_start=DatesBRAnbima().sub_working_days(DatesBRAnbima().curr_date(), 300),
    date_end=DatesBRAnbima().sub_working_days(DatesBRAnbima().curr_date(), 1)
)
df_ = cls_.source("resource", bool_fetch=True)
print(f"DF SGS BCB: \n{df_}")
df_.info()
