"""Example IBGE SIDRA."""

from stpstone.ingestion.countries.br.macroeconomics.ibge_sidra import IBGESIDRA


cls_ = IBGESIDRA(
    list_series_id=None,
    logger=None, 
    cls_db=None
)

df_ = cls_.run()
print(f"DF IBGE SIDRA: \n{df_}")
df_.info()