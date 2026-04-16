"""Yahii Rents - Example of ingestion script."""

from datetime import date

from stpstone.ingestion.countries.br.macroeconomics.yahii_rents import YahiiRentIndices


cls_ = YahiiRentIndices(date_ref=date(1995, 1, 1), logger=None, cls_db=None)

df_ = cls_.run(bool_verify=True)
print(f"DF YAHII RENTS: \n{df_}")
df_.info()
