"""Global-Rates.com EURIBOR interest rates."""

from stpstone.ingestion.countries.ww.macroeconomics.global_rates_euribor import GlobalRatesEuribor


cls_ = GlobalRatesEuribor(
    date_ref=None,
    logger=None,
    cls_db=None,
)

df_ = cls_.run()
print(f"DF GLOBAL RATES EURIBOR: \n{df_}")
df_.info()
