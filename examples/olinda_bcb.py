from stpstone.ingestion.countries.br.macroeconomics.olinda_bcb import OlindaBCB
from stpstone.utils.cals.handling_dates import DatesBR

cls_ = OlindaBCB(
    dt_start=DatesBR().sub_working_days(DatesBR().curr_date, 10),
    dt_end=DatesBR().sub_working_days(DatesBR().curr_date, 1)
)

# df_ = cls_.source("currencies", bl_fetch=True)
# print(f"DF OLINDA BCB - CURRENCIES: \n{df_}")
# df_.info()

df_ = cls_.source("closing_usdbrl_ptax_series", bl_fetch=True)
print(f"DF OLINDA BCB - CLOSING USD/BRL PTAX SERIES: \n{df_}")
df_.info()