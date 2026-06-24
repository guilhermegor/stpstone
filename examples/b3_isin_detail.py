"""B3 ISIN Security Detail Lookup By ISIN Code."""

from stpstone.ingestion.countries.br.registries.b3_isin_detail import B3IsinDetail


cls_ = B3IsinDetail(
	list_isins=["BRBCXPC00294", "BRBCXPC00906", "BRBCXPC008X1"],
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 ISIN DETAIL: \n{df_}")
df_.info()
