"""Global-Rates.com United Kingdom CPI inflation."""

from stpstone.ingestion.countries.ww.macroeconomics.global_rates_british_cpi import (
    GlobalRatesBritishCpi,
)


cls_ = GlobalRatesBritishCpi(
    date_ref=None,
    logger=None,
    cls_db=None,
)

df_ = cls_.run()
print(f"DF GLOBAL RATES BRITISH CPI: \n{df_}")
df_.info()
