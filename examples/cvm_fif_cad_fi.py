"""CVM Liquid Funds Registration Data (CAD/FI) from the CVM CAD dataset."""

from stpstone.ingestion.countries.br.registries.cvm_fif_cad_fi import CvmFIFCADFI


cls_ = CvmFIFCADFI(
    date_ref=None,
    logger=None,
    cls_db=None,
)

df_ = cls_.run()
print(f"DF FIF CAD FI: \n{df_}")
df_.info()
