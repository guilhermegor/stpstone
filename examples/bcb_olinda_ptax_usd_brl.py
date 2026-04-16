"""BCB Olinda PTAX USD/BRL exchange rate time series."""

from stpstone.ingestion.countries.br.macroeconomics.bcb_olinda_ptax_usd_brl import (
    BCBOlindaPTAXUSDBRL,
)


cls_ = BCBOlindaPTAXUSDBRL(
    date_start=None,
    date_end=None,
    logger=None,
    cls_db=None,
)

df_ = cls_.run()
print(f"DF BCB OLINDA PTAX USD BRL: \n{df_}")
df_.info()
