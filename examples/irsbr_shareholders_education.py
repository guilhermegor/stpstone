"""Brazilian IRS (Receita Federal) Shareholders Education (Qualificacoes) Open Data."""

from stpstone.ingestion.countries.br.taxation.irsbr_shareholders_education import (
	IRSBRShareholdersEducation,
)


cls_ = IRSBRShareholdersEducation(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF IRSBR SHAREHOLDERS EDUCATION: \n{df_}")
df_.info()
