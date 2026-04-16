"""Debentures.com.br secondary exchange negotiation prices."""

from stpstone.ingestion.countries.br.otc.debentures_com_br_secondary_exchange import (
    DebenturesComBrSecondaryExchange,
)


cls_ = DebenturesComBrSecondaryExchange(
    date_start=None,
    date_end=None,
    logger=None,
    cls_db=None,
)

df_ = cls_.run()
print(f"DF DEBENTURES COM BR SECONDARY EXCHANGE: \n{df_}")
df_.info()
