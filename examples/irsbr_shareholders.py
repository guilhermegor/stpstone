"""Brazilian IRS (Receita Federal) Shareholders (Socios) Open Data."""

from stpstone.ingestion.countries.br.taxation.irsbr_shareholders import IRSBRShareholders


cls_ = IRSBRShareholders(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF IRSBR SHAREHOLDERS: \n{df_}")
df_.info()
