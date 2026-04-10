"""AlphaVantage daily OHLCV ingestion for US equities."""

from stpstone.ingestion.countries.us.exchange.alphavantage import AlphaVantageUS


cls_ = AlphaVantageUS(
    date_ref=None,
    logger=None,
    cls_db=None,
    token=None,
    list_slugs=["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "TSLA"],
)

df_ = cls_.run()
print(f"DF ALPHAVANTAGE OHLCV: \n{df_}")
df_.info()
