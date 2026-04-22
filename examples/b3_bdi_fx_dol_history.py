"""B3 BDI historical USD/BRL exchange rates."""

from stpstone.ingestion.countries.br.macroeconomics.b3_bdi_fx_dol_history import B3BdiFxDolHistory


cls_ = B3BdiFxDolHistory(
    date_ref=None,
    logger=None,
    cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 BDI FX DOL HISTORY: \n{df_}")
df_.info()
