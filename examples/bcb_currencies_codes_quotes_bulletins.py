"""BCB quotes bulletins currency codes reference data."""

from stpstone.ingestion.countries.br.macroeconomics.bcb_currencies_codes_quotes_bulletins import (
    BCBCurrenciesCodesQuotesBulletins,
)


cls_ = BCBCurrenciesCodesQuotesBulletins(
    date_ref=None,
    logger=None,
    cls_db=None,
)

df_ = cls_.run()
print(f"DF BCB CURRENCIES CODES QUOTES BULLETINS: \n{df_}")
df_.info()
