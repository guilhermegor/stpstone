"""Brazilian IRS (Receita Federal) Businesses (Estabelecimentos) Open Data."""

from stpstone.ingestion.countries.br.taxation.irsbr_businesses import IRSBRBusinesses


cls_ = IRSBRBusinesses(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF IRSBR BUSINESSES: \n{df_}")
df_.info()
