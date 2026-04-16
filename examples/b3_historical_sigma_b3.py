"""Example of ingestion of B3 Historical Sigma data."""

from stpstone.ingestion.countries.br.exchange.b3_historical_sigma import B3HistoricalSigma


cls_ = B3HistoricalSigma(date_ref=None, logger=None, cls_db=None)

df_ = cls_.run()
print(f"DF B3 HISTORICAL SIGMA: \n{df_}")
df_.info()
