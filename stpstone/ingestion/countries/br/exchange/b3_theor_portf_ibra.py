"""B3 IBRA theoretical portfolio ingestion."""

from stpstone.ingestion.abc.ingestion_abc import ABCIngestionOperations
from stpstone.ingestion.countries.br.exchange._b3_theor_portf_base import (
	_B3TheoreticalPortfolioBase,
)


class B3TheoricalPortfolioIBRA(_B3TheoreticalPortfolioBase, ABCIngestionOperations):
	"""B3 IBRA theoretical portfolio ingestion."""

	_URL = (
		"https://sistemaswebb3-listados.b3.com.br/indexProxy/indexCall/GetPortfolioDay/"
		"eyJsYW5ndWFnZSI6InB0LWJyIiwicGFnZU51bWJlciI6MSwicGFnZVNpemUiOjEyMCwiaW5kZXgi"
		"OiJJQlJBIiwic2VnbWVudCI6IjEifQ=="
	)
	_TABLE_NAME = "br_b3_theorical_portfolio_ibra"
