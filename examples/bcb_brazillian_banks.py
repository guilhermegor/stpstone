"""Example of ingestion of Brazilian Banks Codes Compensation from BCB (Central Bank of Brazil)."""

from stpstone.ingestion.countries.br.registries.bcb_brazillian_banks import (
    BCBBanksCodesCompensation,
)


cls_ = BCBBanksCodesCompensation(
    date_ref=None,
    logger=None, 
    cls_db=None
)

df_ = cls_.run()
print(f"DF BCB BANKS CODES COMPENSATION: \n{df_}")
df_.info