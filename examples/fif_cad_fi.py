"""CVM Liquid Funds Registration Data (CAD/FI) from the CVM CAD dataset."""

from stpstone.ingestion.countries.br.registries.fif_cad_fi import FIFCADFI


cls_ = FIFCADFI(
    date_ref=None,
    logger=None,
    cls_db=None,
)

df_ = cls_.run()
print(f"DF FIF CAD FI: \n{df_}")
df_.info()
