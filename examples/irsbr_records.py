"""Brazilian IRS (Receita Federal) Companies (Empresas) Open Data."""

from stpstone.ingestion.countries.br.taxation.irsbr_records import IRSBRCompanies


cls_ = IRSBRCompanies(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF IRSBR COMPANIES: \n{df_}")
df_.info()
