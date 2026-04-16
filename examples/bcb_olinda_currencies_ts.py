"""BCB Olinda multi-currency PTAX quotation time series."""

from stpstone.ingestion.countries.br.macroeconomics.bcb_olinda_currencies_ts import (
    BCBOlindaCurrenciesTS,
)


cls_ = BCBOlindaCurrenciesTS(
    date_start=None,
    date_end=None,
    logger=None,
    cls_db=None,
)

df_ = cls_.run()
print(f"DF BCB OLINDA CURRENCIES TS: \n{df_}")
df_.info()
