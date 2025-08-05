from stpstone.ingestion.countries.br.macroeconomics.olinda_bcb import OlindaBCB
from stpstone.utils.cals.handling_dates import DatesBR


cls_ = OlindaBCB(
    dt_start=DatesBR().sub_working_days(DatesBR().curr_date(), 10),
    dt_end=DatesBR().sub_working_days(DatesBR().curr_date(), 1)
)

# df_ = cls_.source("currencies", bool_fetch=True)
# print(f"DF OLINDA BCB - CURRENCIES: \n{df_}")
# df_.info()

# df_ = cls_.source("closing_usdbrl_ptax_series", bool_l_fetch=True)
# print(f"DF OLINDA BCB - CLOSING USD/BRL PTAX SERIES: \n{df_}")
# df_.info()

# df_ = cls_.source("currencies_ts", bool_l_fetch=True)
# print(f"DF OLINDA BCB - CLOSING USD/BRL PTAX SERIES: \n{df_}")
# df_.info()

df_ = cls_.source("annual_mkt_expectations", bool_l_fetch=True)
print(f"DF OLINDA BCB - ANNUAL MARKET EXPECTATIONS: \n{df_}")
df_.info()
