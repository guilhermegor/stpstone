"""B3 risk formulas applied across derivative and equity portfolios."""

from stpstone.ingestion.countries.br.exchange.b3_risk_formulas import B3RiskFormulas


cls_ = B3RiskFormulas(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 RISK FORMULAS: \n{df_}")
df_.info()
