"""Brazilian IRS (Receita Federal) Countries (Paises) Open Data."""

from stpstone.ingestion.countries.br.taxation.irsbr_countries import IRSBRCountries


cls_ = IRSBRCountries(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF IRSBR COUNTRIES: \n{df_}")
df_.info()
