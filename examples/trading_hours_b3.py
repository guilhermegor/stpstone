# pypi.org libs
import os
# local libs
os.path.abspath(os.path.join(os.path.realpath(__file__), '..'))
from stpstone.ingestion.countries.br.exchange.trading_hours import TradingHoursB3
from stpstone.utils.cals.handling_dates import DatesBR
from stpstone.utils.connections.netops.proxies.managers.free import YieldFreeProxy


session = YieldFreeProxy(
    bl_new_proxy=True,
    bl_use_timer=True,
    float_min_ratio_times_alive_dead=0.02,
    float_max_timeout=600
).session
print(session.proxies)

cls_ = TradingHoursB3(
    session=session,
    cls_db=None
)

print('*** STOCKS TRADING HOURS ***')
df_ = cls_.source('stocks', bl_fetch=True)
print(f'DF : \n{df_}')
df_.info()

print('\n*** STOCK OPTIONS TRADING HOURS ***')
df_ = cls_.source('stock_options', bl_fetch=True)
print(f'DF : \n{df_}')
df_.info()

print('\n*** PMI FUTURES TRADING HOURS ***')
df_ = cls_.source('pmi_future', bl_fetch=True)
print(f'DF : \n{df_}')
df_.info()

print('\n*** STOCK INDEX FUTURES TRADING HOURS ***')
df_ = cls_.source('stock_index_futures', bl_fetch=True)
print(f'DF : \n{df_}')
df_.info()

print('\n*** INTEREST RATES FUTURES TRADING HOURS ***')
df_ = cls_.source('interest_rates', bl_fetch=True)
print(f'DF : \n{df_}')
df_.info()

print('\n*** USD INTEREST RATES FUTURES TRADING HOURS ***')
df_ = cls_.source('usd_interest_rates', bl_fetch=True)
print(f'DF : \n{df_}')
df_.info()

print('\n*** COMMODITIES TRADING HOURS ***')
df_ = cls_.source('commodities', bl_fetch=True)
print(f'DF : \n{df_}')
df_.info()

print('\n*** CRYPTO TRADING HOURS ***')
df_ = cls_.source('crypto', bl_fetch=True)
print(f'DF : \n{df_}')
df_.info()

print('\n*** FOREIGN EXCHANGE AND DOLLAR SPOT TRADING HOURS ***')
df_ = cls_.source('foreign_exchange_and_dollar_spot', bl_fetch=True)
print(f'DF : \n{df_}')
df_.info()

print('\n*** OPF (BEFORE EXERCISE DATE) TRADING HOURS ***')
df_ = cls_.source('opf_before_exc', bl_fetch=True)
print(f'DF : \n{df_}')
df_.info()

print('\n*** OPF (AFTER EXERCISE DATE) TRADING HOURS ***')
df_ = cls_.source('opf_after_exc', bl_fetch=True)
print(f'DF : \n{df_}')
df_.info()
