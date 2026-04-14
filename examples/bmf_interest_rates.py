"""BMF Interest Rates."""

from stpstone.ingestion.countries.br.exchange.bmf_interest_rates import BMFInterestRates


cls_ = BMFInterestRates(date_ref=None, logger=None, cls_db=None)

df_ = cls_.run()
print(f"DF BMF INTEREST RATES: \n{df_}")
df_.info()
