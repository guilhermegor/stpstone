"""B3 time series of file updates published per trading session."""

from stpstone.ingestion.countries.br.exchange.b3_updates_search_by_trading_session_update_time_series import (
	B3UpdatesSearchByTradingSessionUpdateTimeSeries,
)


cls_ = B3UpdatesSearchByTradingSessionUpdateTimeSeries(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 UPDATES SEARCH BY TRADING SESSION UPDATE TIME SERIES: \n{df_}")
df_.info()
