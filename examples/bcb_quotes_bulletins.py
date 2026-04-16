"""BCB quotes bulletins exchange rates for all currencies."""

from stpstone.ingestion.countries.br.macroeconomics.bcb_quotes_bulletins import (
    BCBQuotesBulletins,
)


cls_ = BCBQuotesBulletins(
    date_start=None,
    date_end=None,
    logger=None,
    cls_db=None,
)

df_ = cls_.run()
print(f"DF BCB QUOTES BULLETINS: \n{df_}")
df_.info()
