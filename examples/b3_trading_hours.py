"""B3 Trading Hours."""

from stpstone.ingestion.countries.br.exchange.b3_trading_hours import (
    B3TradingHoursCommoditiesFutures,
    B3TradingHoursCryptoFutures,
    B3TradingHoursExchangeRateFutures,
    B3TradingHoursExerciseBlockingOptionsAfterExerciseDate,
    B3TradingHoursExerciseBlockingOptionsBeforeExerciseDate,
    B3TradingHoursOptionsExercise,
    B3TradingHoursOTC,
    B3TradingHoursPMIFutures,
    B3TradingHoursRealDenominatedInterestRates,
    B3TradingHoursStockIndexFutures,
    B3TradingHoursStocks,
    B3TradingHoursUSDollarDenominatedInterestRatesFutures,
)


cls_ = B3TradingHoursExerciseBlockingOptionsAfterExerciseDate(
    date_ref=None,
    logger=None, 
    cls_db=None
)

df_ = cls_.run(bool_verify=False)
print(f"DF B3 TRADING HOURS EXERCISE BLOCKING OPTIONS AFTER EXERCISE DATE: \n{df_}")
df_.info()


cls_ = B3TradingHoursExerciseBlockingOptionsBeforeExerciseDate(
    date_ref=None,
    logger=None, 
    cls_db=None
)

df_ = cls_.run(bool_verify=False)
print(f"DF B3 TRADING HOURS EXERCISE BLOCKING OPTIONS BEFORE EXERCISE DATE: \n{df_}")
df_.info()


cls_ = B3TradingHoursOTC(
    date_ref=None,
    logger=None, 
    cls_db=None
)

df_ = cls_.run(bool_verify=False)
print(f"DF B3 TRADING HOURS OTC: \n{df_}")
df_.info()


cls_ = B3TradingHoursExchangeRateFutures(
    date_ref=None,
    logger=None, 
    cls_db=None
)

df_ = cls_.run(bool_verify=False)
print(f"DF B3 TRADING HOURS EXCHANGE RATE FUTURES: \n{df_}")
df_.info()


cls_ = B3TradingHoursCryptoFutures(\
    date_ref=None,
    logger=None, 
    cls_db=None    
)

df_ = cls_.run(bool_verify=False)
print(f"DF B3 TRADING HOURS CRYPTO FUTURES: \n{df_}")
df_.info()


cls_ = B3TradingHoursCommoditiesFutures(
    date_ref=None,
    logger=None, 
    cls_db=None
)

df_ = cls_.run(bool_verify=False)
print(f"DF B3 TRADING HOURS COMMODITIES FUTURES: \n{df_}")
df_.info()


cls_ = B3TradingHoursUSDollarDenominatedInterestRatesFutures(
    date_ref=None,
    logger=None, 
    cls_db=None
)

df_ = cls_.run(bool_verify=False)
print(f"DF B3 TRADING HOURS USDOLLAR DENOMINATED INTEREST RATES FUTURES: \n{df_}")
df_.info()


cls_ = B3TradingHoursRealDenominatedInterestRates(
    date_ref=None,
    logger=None, 
    cls_db=None
)

df_ = cls_.run(bool_verify=False)
print(f"DF B3 TRADING HOURS REAL DENOMINATED INTEREST RATES: \n{df_}")
df_.info()

cls_ = B3TradingHoursPMIFutures(
    date_ref=None,
    logger=None, 
    cls_db=None
)

df_ = cls_.run(bool_verify=False)
print(f"DF B3 TRADING HOURS PMI FUTURES: \n{df_}")
df_.info()


cls_ = B3TradingHoursStockIndexFutures(
    date_ref=None,
    logger=None, 
    cls_db=None
)

df_ = cls_.run(bool_verify=False)
print(f"DF B3 TRADING HOURS STOCK INDEX FUTURES: \n{df_}")
df_.info()


cls_ = B3TradingHoursStockIndexFutures(
    date_ref=None,
    logger=None, 
    cls_db=None
)

df_ = cls_.run(bool_verify=False)
print(f"DF B3 TRADING HOURS STOCK INDEX FUTURES: \n{df_}")
df_.info()


cls_ = B3TradingHoursStocks(
    date_ref=None,
    logger=None, 
    cls_db=None
)

df_ = cls_.run(bool_verify=False)
print(f"DF B3 TRADING HOURS STOCKS: \n{df_}")
df_.info()


cls_ = B3TradingHoursOptionsExercise(
    date_ref=None,
    logger=None, 
    cls_db=None
)

df_ = cls_.run(bool_verify=False)
print(f"DF B3 TRADING HOURS OPTIONS EXERCISE: \n{df_}")
df_.info()