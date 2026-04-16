"""B3 instruments file for options on spot and futures."""

from stpstone.ingestion.countries.br.exchange.b3_instruments_file_optn_on_spot_and_futures import (
	B3InstrumentsFileOptnOnSpotAndFutures,
)


cls_ = B3InstrumentsFileOptnOnSpotAndFutures(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 INSTRUMENTS FILE OPTN ON SPOT AND FUTURES: \n{df_}")
df_.info()
