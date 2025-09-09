"""B3 Search by Trading Ingestion."""

from stpstone.ingestion.countries.br.exchange.b3_search_by_trading_session import (
    B3IndexReport,
    B3InstrumentsFileADR,
    B3InstrumentsFileBTC,
    B3InstrumentsFileEqty,
    B3InstrumentsFileEqtyFwd,
    B3InstrumentsFileExrcEqts,
    B3InstrumentsFileFxdIncm,
    B3InstrumentsFileIndicators,
    B3InstrumentsFileOptnOnEqts,
    B3InstrumentsFileOptnOnSpotAndFutrs,
    B3PriceReport,
    B3StandardizedInstrumentGroups,
)


cls_ = B3InstrumentsFileIndicators(
    date_ref=None,
    logger=None, 
    cls_db=None
)

df_ = cls_.run(bool_verify=False)
print(f"DF B3 INSTRUMENTS FILE INDICATORS: \n{df_}")
df_.info()


cls_ = B3InstrumentsFileEqty(
    date_ref=None,
    logger=None, 
    cls_db=None
)

df_ = cls_.run(bool_verify=False)
print(f"DF B3 INSTRUMENTS FILE EQTY: \n{df_}")
df_.info()


cls_ = B3InstrumentsFileOptnOnEqts(
    date_ref=None,
    logger=None, 
    cls_db=None
)

df_ = cls_.run(bool_verify=False)
print(f"DF B3 INSTRUMENTS FILE OPTN ON EQTS: \n{df_}")
df_.info()


cls_ = B3InstrumentsFileOptnOnSpotAndFutrs(
    date_ref=None,
    logger=None, 
    cls_db=None
)

df_ = cls_.run(bool_verify=False)
print(f"DF B3 INSTRUMENTS FILE OPTN ON SPOT AND FUTRS: \n{df_}")
df_.info()


# cls_ = B3PriceReport(
#     date_ref=None,
#     logger=None, 
#     cls_db=None
# )

# df_ = cls_.run(bool_verify=False)
# print(f"DF B3 PRICE REPORT: \n{df_}")
# df_.info()


# cls_ = B3IndexReport(
#     date_ref=None,
#     logger=None, 
#     cls_db=None
# )

# df_ = cls_.run(bool_verify=False)
# print(f"DF B3 INDEX REPORT: \n{df_}")
# df_.info()


# cls_ = B3StandardizedInstrumentGroups(
#     date_ref=None,
#     logger=None, 
#     cls_db=None
# )

# df_ = cls_.run(bool_verify=False)
# print(f"DF B3 STANDARDIZED INSTRUMENT GROUPS: \n{df_}")
# df_.info()