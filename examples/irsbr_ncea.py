"""Brazilian IRS (Receita Federal) NCEA (Cnaes) Open Data."""

from stpstone.ingestion.countries.br.taxation.irsbr_ncea import IRSBRNCEA


cls_ = IRSBRNCEA(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF IRSBR NCEA: \n{df_}")
df_.info()
