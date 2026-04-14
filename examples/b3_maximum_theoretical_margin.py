"""B3 maximum theoretical margin per instrument for risk assessment."""

from stpstone.ingestion.countries.br.exchange.b3_maximum_theoretical_margin import (
	B3MaximumTheoreticalMargin,
)


cls_ = B3MaximumTheoreticalMargin(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 MAXIMUM THEORETICAL MARGIN: \n{df_}")
df_.info()
