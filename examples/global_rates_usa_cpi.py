"""Global-Rates.com United States CPI inflation."""

from stpstone.ingestion.countries.ww.macroeconomics.global_rates_usa_cpi import GlobalRatesUsaCpi


cls_ = GlobalRatesUsaCpi(
    date_ref=None,
    logger=None,
    cls_db=None,
)

df_ = cls_.run()
print(f"DF GLOBAL RATES USA CPI: \n{df_}")
df_.info()
