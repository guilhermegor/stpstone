"""Example of ingestion of B3 Options Settlment Calendar."""

from stpstone.ingestion.countries.br.exchange.b3_options_settlement_calendar import (
    B3OptionsSettlementCalendar,
)


cls_ = B3OptionsSettlementCalendar(
    date_ref=None, 
    logger=None,
    cls_db=None,
)
df_ = cls_.run()
print(f"DF B3 OPTIONS SETTLEMENT CALENDAR: \n{df_}")
df_.info