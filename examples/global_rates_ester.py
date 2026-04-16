"""Global-Rates.com ESTER overnight rate."""

from stpstone.ingestion.countries.ww.macroeconomics.global_rates_ester import GlobalRatesEster


cls_ = GlobalRatesEster(
    date_ref=None,
    logger=None,
    cls_db=None,
)

df_ = cls_.run()
print(f"DF GLOBAL RATES ESTER: \n{df_}")
df_.info()
