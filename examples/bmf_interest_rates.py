from stpstone.ingestion.countries.br.exchange.bmf_interest_rates import BMFInterestRates
from stpstone.utils.calendars.calendar_br import DatesBRAnbima


cls_ = BMFInterestRates(
    session=None,
    date_ref=DatesBRAnbima().sub_working_days(DatesBRAnbima().curr_date(), 1),
    cls_db=None
)

df_ = cls_.source("rates", bool_fetch=True)
print(f"DF BMF INTEREST RATES: \n{df_}")
df_.info()
