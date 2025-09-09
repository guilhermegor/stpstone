"""B3 Search by Trading Ingestion."""

from stpstone.ingestion.countries.br.exchange.b3_search_by_trading_session import (
    B3IndexReport,
    B3PriceReport,
    B3StandardizedInstrumentGroups,
)


cls_ = B3PriceReport(
    date_ref=None,
    logger=None, 
    cls_db=None
)

df_ = cls_.run(bool_verify=False)
print(f"DF B3 PRICE REPORT: \n{df_}")
df_.info()


cls_ = B3IndexReport(
    date_ref=None,
    logger=None, 
    cls_db=None
)

df_ = cls_.run(bool_verify=False)
print(f"DF B3 INDEX REPORT: \n{df_}")
df_.info()


cls_ = B3StandardizedInstrumentGroups(
    date_ref=None,
    logger=None, 
    cls_db=None
)

df_ = cls_.run(bool_verify=False)
print(f"DF B3 STANDARDIZED INSTRUMENT GROUPS: \n{df_}")
df_.info()