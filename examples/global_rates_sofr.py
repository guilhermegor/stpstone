"""Global-Rates.com SOFR overnight rate."""

from stpstone.ingestion.countries.ww.macroeconomics.global_rates_sofr import GlobalRatesSofr


cls_ = GlobalRatesSofr(
    date_ref=None,
    logger=None,
    cls_db=None,
)

df_ = cls_.run()
print(f"DF GLOBAL RATES SOFR: \n{df_}")
df_.info()
