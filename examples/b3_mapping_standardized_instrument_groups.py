"""B3 mapping of standardized instrument groups with validity intervals."""

from stpstone.ingestion.countries.br.exchange.b3_mapping_standardized_instrument_groups import (
	B3MappingStandardizedInstrumentGroups,
)


cls_ = B3MappingStandardizedInstrumentGroups(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 MAPPING STANDARDIZED INSTRUMENT GROUPS: \n{df_}")
df_.info()
