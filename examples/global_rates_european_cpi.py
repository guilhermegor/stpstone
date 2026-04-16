"""Global-Rates.com European HICP inflation."""

from stpstone.ingestion.countries.ww.macroeconomics.global_rates_european_cpi import (
    GlobalRatesEuropeanCpi,
)


cls_ = GlobalRatesEuropeanCpi(
    date_ref=None,
    logger=None,
    cls_db=None,
)

df_ = cls_.run()
print(f"DF GLOBAL RATES EUROPEAN CPI: \n{df_}")
df_.info()
