"""BCB PTAX currency codes reference data."""

from stpstone.ingestion.countries.br.macroeconomics.bcb_currencies_codes_ptax import (
    BCBCurrenciesCodesPTAX,
)


cls_ = BCBCurrenciesCodesPTAX(
    date_ref=None,
    logger=None,
    cls_db=None,
)

df_ = cls_.run()
print(f"DF BCB CURRENCIES CODES PTAX: \n{df_}")
df_.info()
