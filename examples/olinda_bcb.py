from stpstone.ingestion.countries.br.macroeconomics.olinda_bcb import OlindaBCB
from stpstone.utils.calendars.calendar_br import DatesBRAnbima


cls_ = OlindaBCB(
    date_start=DatesBRAnbima().sub_working_days(DatesBRAnbima().curr_date(), 10),
    date_end=DatesBRAnbima().sub_working_days(DatesBRAnbima().curr_date(), 1)
)

# df_ = cls_.source("currencies", bool_fetch=True)
# print(f"DF OLINDA BCB - CURRENCIES: \n{df_}")
# df_.info()

# df_ = cls_.source("closing_usdbrl_ptax_series", bool_fetch=True)
# print(f"DF OLINDA BCB - CLOSING USD/BRL PTAX SERIES: \n{df_}")
# df_.info()

# df_ = cls_.source("currencies_ts", bool_fetch=True)
# print(f"DF OLINDA BCB - CLOSING USD/BRL PTAX SERIES: \n{df_}")
# df_.info()

df_ = cls_.source("annual_mkt_expectations", bool_fetch=True)
print(f"DF OLINDA BCB - ANNUAL MARKET EXPECTATIONS: \n{df_}")
df_.info()
