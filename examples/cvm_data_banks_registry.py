"""CVM Financial Intermediaries Registry from the CVM INTERMED dataset."""

from stpstone.ingestion.countries.br.registries.cvm_data_banks_registry import (
    CVMDataBanksRegistry,
)


cls_ = CVMDataBanksRegistry(
    date_ref=None,
    logger=None,
    cls_db=None,
)

df_ = cls_.run()
print(f"DF CVM Data Banks Registry: \n{df_}")
df_.info()
