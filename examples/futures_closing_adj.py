"""Futures Closing Adjustments."""

from stpstone.ingestion.countries.br.exchange.futures_closing_adj import B3FuturesClosingAdj


cls_ = B3FuturesClosingAdj(
    date_ref=None, 
    logger=None, 
    cls_db=None
)

df_ = cls_.run()

print(f"DF FUTURES CLOSING ADJ B3: \n{df_}")
df_.info()