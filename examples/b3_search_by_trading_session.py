"""B3 Search by Trading Ingestion."""

from stpstone.ingestion.countries.br.exchange.b3_search_by_trading_session import (
    B3DailyLiquidityLimits,
    B3FeeDailyUnitCost,
    B3FeeUnitCost,
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
    B3MappingOTCInstrumentGroups,
    B3MappingStandardizedInstrumentGroups,
    B3MaximumTheoreticalMargin,
    B3OtherDailyLiquidityLimits,
    B3PriceReport,
    B3PrimitiveRiskFactors,
    B3RiskFormulas,
    B3StandardizedInstrumentGroups,
    B3TradableSecurityList,
    B3VariableFees,
)


cls_ = B3MaximumTheoreticalMargin(
    date_ref=None,
    logger=None, 
    cls_db=None
)

df_ = cls_.run(bool_verify=False)
print(f"DF B3 MAXIMUM THEORETICAL MARGIN: \n{df_}")
df_.info()


cls_ = B3MappingStandardizedInstrumentGroups(
    date_ref=None,
    logger=None, 
    cls_db=None
)

df_ = cls_.run(bool_verify=False)
print(f"DF B3 MAPPING STANDARDIZED INSTRUMENT GROUPS: \n{df_}")
df_.info()

cls_ = B3MappingOTCInstrumentGroups(
    date_ref=None,
    logger=None, 
    cls_db=None
)

df_ = cls_.run(bool_verify=False)
print(f"DF B3 MAPPING OTC INSTRUMENT GROUPS: \n{df_}")
df_.info()


cls_ = B3TradableSecurityList(
    date_ref=None,
    logger=None, 
    cls_db=None
)

df_ = cls_.run(bool_verify=False)
print(f"DF B3 TRADEABLE SECURITY LIST: \n{df_}")
df_.info()


cls_ = B3OtherDailyLiquidityLimits(
    date_ref=None,
    logger=None, 
    cls_db=None
)

df_ = cls_.run(bool_verify=False)
print(f"DF B3 OTHER DAILY LIQUIDITY LIMITS: \n{df_}")
df_.info()


cls_ = B3DailyLiquidityLimits(
    date_ref=None,
    logger=None, 
    cls_db=None
)

df_ = cls_.run(bool_verify=False)
print(f"DF B3 DAILY LIQUIDITY LIMITS: \n{df_}")
df_.info()


cls_ = B3VariableFees(
    date_ref=None,
    logger=None, 
    cls_db=None
)

df_ = cls_.run(bool_verify=False)
print(f"DF B3 VARIABLE FEES: \n{df_}")
df_.info()

cls_ = B3RiskFormulas(
    date_ref=None,
    logger=None, 
    cls_db=None
)

df_ = cls_.run(bool_verify=False)
print(f"DF B3 RISK FORMULAS: \n{df_}")
df_.info()


cls_ = B3PrimitiveRiskFactors(
    date_ref=None,
    logger=None, 
    cls_db=None
)

df_ = cls_.run(bool_verify=False)
print(f"DF B3 PRIMITIVE RISK FACTORS: \n{df_}")
df_.info()


cls_ = B3FeeUnitCost(
    date_ref=None,
    logger=None, 
    cls_db=None
)

df_ = cls_.run(bool_verify=False)
print(f"DF B3 FEE UNIT COST: \n{df_}")
df_.info()


cls_ = B3FeeDailyUnitCost(
    date_ref=None,
    logger=None, 
    cls_db=None
)

df_ = cls_.run(bool_verify=False)
print(f"DF B3 FEE DAILY UNIT COST: \n{df_}")
df_.info()


cls_ = B3InstrumentsFileIndicators(
    date_ref=None,
    logger=None, 
    cls_db=None
)

df_ = cls_.run(bool_verify=False)
print(f"DF B3 INSTRUMENTS FILE INDICATORS: \n{df_}")
df_.info()


cls_ = B3InstrumentsFileEqtyFwd(
    date_ref=None,
    logger=None, 
    cls_db=None
)

df_ = cls_.run(bool_verify=False)
print(f"DF B3 INSTRUMENTS FILE EQTY FWD: \n{df_}")
df_.info()


cls_ = B3InstrumentsFileBTC(
    date_ref=None,
    logger=None, 
    cls_db=None
)

df_ = cls_.run(bool_verify=False)
print(f"DF B3 INSTRUMENTS FILE BTC: \n{df_}")
df_.info()


cls_ = B3InstrumentsFileADR(
    date_ref=None,
    logger=None, 
    cls_db=None
)

df_ = cls_.run(bool_verify=False)
print(f"DF B3 INSTRUMENTS FILE ADR: \n{df_}")
df_.info()


cls_ = B3InstrumentsFileFxdIncm(
    date_ref=None,
    logger=None, 
    cls_db=None
)

df_ = cls_.run(bool_verify=False)
print(f"DF B3 INSTRUMENTS FILE FXD INCM: \n{df_}")
df_.info()


cls_ = B3InstrumentsFileExrcEqts(
    date_ref=None,
    logger=None, 
    cls_db=None
)

df_ = cls_.run(bool_verify=False)
print(f"DF B3 INSTRUMENTS FILE EXRC EQTS: \n{df_}")
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