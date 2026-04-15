"""CVM Liquid Funds Annual Statement (Extrato) from the FIF Extrato dataset."""

from stpstone.ingestion.countries.br.registries.fif_statement import FIFStatement


cls_ = FIFStatement(
    date_ref=None,
    logger=None,
    cls_db=None,
)

df_ = cls_.run()
print(f"DF FIF Statement: \n{df_}")
df_.info()
