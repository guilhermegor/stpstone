"""B3 mapping of OTC instrument groups to risk factor formulas."""

from stpstone.ingestion.countries.br.exchange.b3_mapping_otc_instrument_groups import (
	B3MappingOTCInstrumentGroups,
)


cls_ = B3MappingOTCInstrumentGroups(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 MAPPING O T C INSTRUMENT GROUPS: \n{df_}")
df_.info()
