"""Global-Rates.com Canada CPI inflation."""

from stpstone.ingestion.countries.ww.macroeconomics.global_rates_canadian_cpi import (
    GlobalRatesCanadianCpi,
)


cls_ = GlobalRatesCanadianCpi(
    date_ref=None,
    logger=None,
    cls_db=None,
)

df_ = cls_.run()
print(f"DF GLOBAL RATES CANADIAN CPI: \n{df_}")
df_.info()
