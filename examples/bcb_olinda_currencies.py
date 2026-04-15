"""BCB Olinda list of currencies available for PTAX quotation."""

from stpstone.ingestion.countries.br.macroeconomics.bcb_olinda_currencies import (
    BCBOlindaCurrencies,
)


cls_ = BCBOlindaCurrencies(
    date_ref=None,
    logger=None,
    cls_db=None,
)

df_ = cls_.run()
print(f"DF BCB OLINDA CURRENCIES: \n{df_}")
df_.info()
