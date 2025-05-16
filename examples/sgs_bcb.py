from stpstone.ingestion.countries.br.macroeconomics.sgs_bcb import SGSBCB
from stpstone.utils.cals.handling_dates import DatesBR


cls_ = SGSBCB(
    dt_start=DatesBR().sub_working_days(DatesBR().curr_date, 300),
    dt_end=DatesBR().sub_working_days(DatesBR().curr_date, 1)
)
df_ = cls_.source("resource", bl_fetch=True)
print(f"DF SGS BCB: \n{df_}")
df_.info()