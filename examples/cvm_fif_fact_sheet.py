"""CVM Liquid Funds Fact Sheet (Lamina) from the FIF Lamina dataset."""

from stpstone.ingestion.countries.br.registries.cvm_fif_fact_sheet import CvmFIFFactSheet


cls_ = CvmFIFFactSheet(
    date_ref=None,
    logger=None,
    cls_db=None,
)

df_ = cls_.run()
print(f"DF FIF Fact Sheet: \n{df_}")
df_.info()
