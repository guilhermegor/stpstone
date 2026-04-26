"""Anbima Data Funds About — detailed information for individual funds on ANBIMA Data portal."""

from stpstone.ingestion.countries.br.registries.anbima_data_funds_about import (
	AnbimaDataFundsAbout,
)


cls_ = AnbimaDataFundsAbout(
	date_ref=None,
	logger=None,
	cls_db=None,
	list_fund_codes=["S0000634344"],
)

dict_ = cls_.run()
df_characteristics, df_related, df_about = (
	dict_["characteristics"],
	dict_["related"],
	dict_["about"],
)

print(f"DF ANBIMA DATA FUNDS CHARACTERISTICS: \n{df_characteristics}")
df_characteristics.info()

print(f"DF ANBIMA DATA FUNDS RELATED: \n{df_related}")
df_related.info()

print(f"DF ANBIMA DATA FUNDS ABOUT: \n{df_about}")
df_about.info()
