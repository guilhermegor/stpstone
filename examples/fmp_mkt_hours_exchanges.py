"""FMP market hours for all exchanges."""

from stpstone.ingestion.countries.ww.exchange.markets.fmp_mkt_hours_exchanges import (
	FMPMktHoursExchanges,
)


cls_ = FMPMktHoursExchanges(
	token="YOUR_FMP_TOKEN",
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF FMP Market Hours Exchanges: \n{df_}")
df_.info()
