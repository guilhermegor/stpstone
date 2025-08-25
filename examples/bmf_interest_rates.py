from stpstone.ingestion.countries.br.exchange.bmf_interest_rates import BMFInterestRates
from stpstone.utils.cals.cal_abc import DatesBR


cls_ = BMFInterestRates(
    session=None,
    date_ref=DatesBR().sub_working_days(DatesBR().curr_date(), 1),
    cls_db=None
)

df_ = cls_.source("rates", bool_fetch=True)
print(f"DF BMF INTEREST RATES: \n{df_}")
df_.info()
