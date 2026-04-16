"""Brazilian IRS (Receita Federal) Cities (Municipios) Open Data."""

from stpstone.ingestion.countries.br.taxation.irsbr_cities import IRSBRCities


cls_ = IRSBRCities(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF IRSBR CITIES: \n{df_}")
df_.info()
