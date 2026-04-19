"""B3 BDI Margin Scenarios for liquid assets - PRF values and shock types by scenario and vertex."""

from stpstone.ingestion.countries.br.exchange.b3_bdi_stocks_margin_scenarios_liquid_assets import (
	B3BdiStocksMarginScenariosLiquidAssets,
)


cls_ = B3BdiStocksMarginScenariosLiquidAssets(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 BDI STOCKS MARGIN SCENARIOS LIQUID ASSETS: \n{df_}")
df_.info()
