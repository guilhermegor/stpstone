"""Global-Rates.com SONIA overnight rate."""

from stpstone.ingestion.countries.ww.macroeconomics.global_rates_sonia import GlobalRatesSonia


cls_ = GlobalRatesSonia(
    date_ref=None,
    logger=None,
    cls_db=None,
)

df_ = cls_.run()
print(f"DF GLOBAL RATES SONIA: \n{df_}")
df_.info()
