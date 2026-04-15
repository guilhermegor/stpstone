"""B3 Warranties - Stocks, Units and ETFs Accepted as Collateral."""

from stpstone.ingestion.countries.br.exchange.b3_warranties_stocks_units_etfs import (
	B3WarrantiesStocksUnitsETFs,
)


cls_ = B3WarrantiesStocksUnitsETFs(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 WARRANTIES STOCKS UNITS ETFs: \n{df_}")
df_.info()
