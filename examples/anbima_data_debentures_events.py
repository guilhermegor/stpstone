"""Anbima Debentures events data."""

from stpstone.ingestion.countries.br.registries.anbima_data_debentures_events import (
	AnbimaDataDebenturesEvents,
)


cls_ = AnbimaDataDebenturesEvents(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF ANBIMA DEBENTURES EVENTS: \n{df_}")
df_.info()
