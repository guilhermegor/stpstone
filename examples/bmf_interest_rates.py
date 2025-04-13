from stpstone.ingestion.countries.br.exchange.bmf_interest_rates import BMFInterestRates
from stpstone.utils.cals.handling_dates import DatesBR


cls_ = BMFInterestRates(
    session=None,
    dt_ref=DatesBR().sub_working_days(DatesBR().curr_date, 1),
    cls_db=None
)

df_ = cls_.source("rates", bl_fetch=True)
print(f"DF BMF INTEREST RATES: \n{df_}")
df_.info()
