"""B3 Search by Trading Ingestion."""

from datetime import date

from stpstone.ingestion.countries.br.exchange.b3_search_by_trading_session import (
    B3DailyLiquidityLimits,
    B3DerivatiesMarketListISINCPRs,
    B3DerivativesMarketCombinedPositions,
    B3DerivativesMarketConsiderationFactors,
    B3DerivativesMarketDollarSwap,
    B3DerivativesMarketEconomicAgriculturalIndicators,
    B3DerivativesMarketListISINDerivativesContracts,
    B3DerivativesMarketListISINSwaps,
    B3DerivativesMarketMarginScenarios,
    B3DerivativesMarketOptionReferencePremium,
    B3DerivativesMarketOTCMarketTrades,
    B3DerivativesMarketSwapMarketRates,
    B3EquitiesFeePublicInformation,
    B3EquitiesOptionReferencePremiums,
    B3FeeDailyUnitCost,
    B3FeeUnitCost,
    B3FixedIncome,
    B3FXMarketContractedTransactions,
    B3FXMarketVolumeSettled,
    B3IndexReport,
    B3InstrumentGroupParameters,
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
    B3SecuritiesMarketGovernmentSecuritiesPrices,
    B3StandardizedInstrumentGroups,
    B3TradableSecurityList,
    B3VariableFees,
)


cls_ = B3EquitiesFeePublicInformation(
    date_ref=date(2025, 9, 1),
    logger=None, 
    cls_db=None
)

df_ = cls_.run(bool_verify=False)
print(f"DF B3 EQUITIES FEE PUBLIC INFORMATION: \n{df_}")
df_.info()


cls_ = B3FixedIncome(
    date_ref=None,
    logger=None, 
    cls_db=None
)

df_ = cls_.run(bool_verify=False)
print(f"DF B3 FIXED INCOME: \n{df_}")
df_.info()


cls_ = B3InstrumentGroupParameters(
    date_ref=None,
    logger=None, 
    cls_db=None
)

df_ = cls_.run(bool_verify=False)
print(f"DF B3 INSTRUMENT GROUP PARAMETERS: \n{df_}")
df_.info()


cls_ = B3SecuritiesMarketGovernmentSecuritiesPrices(
    date_ref=None,
    logger=None, 
    cls_db=None
)

df_ = cls_.run(bool_verify=False)
print(f"DF B3 SECURITIES MARKET GOVERNMENT SECURITIES PRICES: \n{df_}")
df_.info()


cls_ = B3DerivativesMarketSwapMarketRates(
    date_ref=None,
    logger=None, 
    cls_db=None
)

df_ = cls_.run(bool_verify=False)
print(f"DF B3 DERIVATIVES MARKET SWAP MARKET RATES: \n{df_}")
df_.info()


cls_ = B3DerivativesMarketDollarSwap(
    date_ref=None,
    logger=None, 
    cls_db=None
)

df_ = cls_.run(bool_verify=False)
print(f"DF B3 DERIVATIVES MARKET DOLLAR SWAP: \n{df_}")
df_.info()


cls_ = B3DerivativesMarketListISINSwaps(
    date_ref=None,
    logger=None, 
    cls_db=None
)

df_ = cls_.run(bool_verify=False)
print(f"DF B3 DERIVATIVES MARKET LIST ISIN SWAPS: \n{df_}")
df_.info()


cls_ = B3DerivativesMarketListISINDerivativesContracts(
    date_ref=None,
    logger=None, 
    cls_db=None
)

df_ = cls_.run(bool_verify=False)
print(f"DF B3 DERIVATIVES MARKET LIST ISIN DERIVATIVES CONTRACTS: \n{df_}")
df_.info()


cls_ = B3DerivatiesMarketListISINCPRs(
    date_ref=None,
    logger=None, 
    cls_db=None
)

df_ = cls_.run(bool_verify=False)
print(f"DF B3 DERIVATIVES MARKET LIST ISINCPRS: \n{df_}")
df_.info()


cls_ = B3DerivativesMarketOptionReferencePremium(
    date_ref=None,
    logger=None, 
    cls_db=None
)

df_ = cls_.run(bool_verify=False)
print(f"DF B3 DERIVATIVES MARKET OPTION REFERENCE PREMIUM: \n{df_}")
df_.info()


cls_ = B3DerivativesMarketCombinedPositions(
    date_ref=None,
    logger=None, 
    cls_db=None
)

df_ = cls_.run(bool_verify=False)
print(f"DF B3 DERIVATIVES MARKET COMBINED POSITIONS: \n{df_}")
df_.info()


cls_ = B3DerivativesMarketOTCMarketTrades(
    date_ref=None,
    logger=None, 
    cls_db=None
)

df_ = cls_.run(bool_verify=False)
print(f"DF B3 DERIVATIVES MARKET OTC MARKET TRADES: \n{df_}")
df_.info()


cls_ = B3EquitiesOptionReferencePremiums(
    date_ref=None,
    logger=None, 
    cls_db=None
)

df_ = cls_.run(bool_verify=False)
print(f"DF B3 EQUITIES OPTION REFERENCE PREMIUMS: \n{df_}")
df_.info()


cls_ = B3DerivativesMarketEconomicAgriculturalIndicators(
    date_ref=None,
    logger=None, 
    cls_db=None
)

df_ = cls_.run(bool_verify=False)
print(f"DF B3 DERIVATIVES MARKET ECONOMIC AGRICULTURAL INDICATORS: \n{df_}")
df_.info()


cls_ = B3DerivativesMarketConsiderationFactors(
    date_ref=None,
    logger=None, 
    cls_db=None
)

df_ = cls_.run(bool_verify=False)
print(f"DF B3 DERIVATIVES MARKET CONSIDERATION FACTORS: \n{df_.head(30)}")
df_.info()


cls_ = B3DerivativesMarketMarginScenarios(
    date_ref=None,
    logger=None, 
    cls_db=None
)

df_ = cls_.run(bool_verify=False)
print(f"DF B3 DERIVATIVES MARKET MARGIN SCENARIOS: \n{df_}")
df_.info()

cls_ = B3FXMarketVolumeSettled(
    date_ref=None,
    logger=None, 
    cls_db=None
)

df_ = cls_.run(bool_verify=False)
print(f"DF B3 FX MARKET VOLUME SETTLED: \n{df_}")
df_.info()


cls_ = B3FXMarketContractedTransactions(
    date_ref=None,
    logger=None, 
    cls_db=None
)

df_ = cls_.run(bool_verify=False)
print(f"DF B3 FX MARKET CONTRACTED TRANSACTIONS: \n{df_}")
df_.info()


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