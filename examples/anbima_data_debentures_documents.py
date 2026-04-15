"""Anbima Debentures documents data."""

from stpstone.ingestion.countries.br.registries.anbima_data_debentures_documents import (
	AnbimaDataDebenturesDocuments,
)


cls_ = AnbimaDataDebenturesDocuments(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF ANBIMA DEBENTURES DOCUMENTS: \n{df_}")
df_.info()
