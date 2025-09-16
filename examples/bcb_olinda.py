"""Example of ingestion of BCB Olinda Currencies data."""

from stpstone.ingestion.countries.br.macroeconomics.bcb_olinda import (
    BCBOlindaCurrencies,
    BCBOlindaPTAXUSDBRL,
)


cls_ = BCBOlindaPTAXUSDBRL(
    date_start=None,
    date_end=None,
    logger=None, 
    cls_db=None
)

df_ = cls_.run(bool_verify=False)
print(f"DF BCB OLINDA CURRENCIES: \n{df_}")
df_.info()


cls_ = BCBOlindaCurrencies(
    date_ref=None,
    logger=None, 
    cls_db=None
)

df_ = cls_.run(bool_verify=False)
print(f"DF BCB OLINDA CURRENCIES: \n{df_}")
df_.info()