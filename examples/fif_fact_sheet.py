"""CVM Liquid Funds Fact Sheet (Lamina) from the FIF Lamina dataset."""

from stpstone.ingestion.countries.br.registries.fif_fact_sheet import FIFFactSheet


cls_ = FIFFactSheet(
    date_ref=None,
    logger=None,
    cls_db=None,
)

df_ = cls_.run()
print(f"DF FIF Fact Sheet: \n{df_}")
df_.info()
