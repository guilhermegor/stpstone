"""Investing.com ticker ID lookup for worldwide exchange-traded instruments."""

from stpstone.ingestion.countries.ww.exchange.markets.investingcom_ticker_id import InvestingComTickerId


cls_ = InvestingComTickerId(
	str_ticker="PETR4",
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF INVESTING.COM TICKER ID: \n{df_}")
df_.info()
