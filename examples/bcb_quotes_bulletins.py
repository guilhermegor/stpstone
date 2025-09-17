"""Example for BCBQuotesBulletins."""

from stpstone.ingestion.countries.br.macroeconomics.bcb_quotes_bulletins import (
    BCBCurrenciesCodesPTAX,
    BCBQuotesBulletins,
)


cls_ = BCBCurrenciesCodesPTAX(
    date_ref=None,
    logger=None, 
    cls_db=None,
)

df_ = cls_.run()
print(f"DF BCB CURRENCIES CODES: \n{df_}")
df_.info()


cls_ = BCBQuotesBulletins(
    date_start=None,
    date_end=None,
    logger=None, 
    cls_db=None,
)

df_ = cls_.run()
print(f"DF BCB QUOTES BULLETINS: \n{df_}")
df_.info()