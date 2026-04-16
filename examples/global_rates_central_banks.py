"""Global-Rates.com central bank policy rates."""

from stpstone.ingestion.countries.ww.macroeconomics.global_rates_central_banks import (
    GlobalRatesCentralBanks,
)


cls_ = GlobalRatesCentralBanks(
    date_ref=None,
    logger=None,
    cls_db=None,
)

df_ = cls_.run()
print(f"DF GLOBAL RATES CENTRAL BANKS: \n{df_}")
df_.info()
