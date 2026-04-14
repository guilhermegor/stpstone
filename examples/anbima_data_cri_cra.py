"""Example of Anbima data CRI/CRA."""

from stpstone.ingestion.countries.br.registries.anbima_data_cri_cra import (
	AnbimaDataCRICRACharacteristics,
	AnbimaDataCRICRADocuments,
	AnbimaDataCRICRAEvents,
	AnbimaDataCRICRAIndividualCharacteristics,
	AnbimaDataCRICRAPricesFile,
	AnbimaDataCRICRAPricesWS,
	AnbimaDataCRICRAPUHistorico,
	AnbimaDataCRICRAPUIndicativo,
)


cls_ = AnbimaDataCRICRAEvents(
	date_ref=None,
	logger=None,
	cls_db=None,
	list_asset_codes=["18L1085826", "19C0000001", "CRA019000GT"],
)

df_ = cls_.run()
print(f"DF ANBIMA CRI/CRA EVENTS: \n{df_}")
df_.info()


# cls_ = AnbimaDataCRICRAPUIndicativo(
#     date_ref=None,
#     logger=None,
#     cls_db=None,
#     list_asset_codes=["18L1085826", "19C0000001", "CRA019000GT"],
# )

# df_ = cls_.run()
# print(f"DF ANBIMA CRI/CRA PU INDICATIVO: \n{df_}")
# df_.info()


# cls_ = AnbimaDataCRICRAPUHistorico(
#     date_ref=None,
#     logger=None,
#     cls_db=None,
#     list_asset_codes=["18L1085826", "19C0000001", "CRA019000GT"],
# )

# df_ = cls_.run()
# print(f"DF ANBIMA CRI/CRA PU HISTORICO: \n{df_}")
# df_.info()


# cls_ = AnbimaDataCRICRADocuments(
#     date_ref=None,
#     logger=None,
#     cls_db=None,
#     list_asset_codes=["18L1085826", "19C0000001", "CRA019000GT"],
# )

# df_ = cls_.run()
# print(f"DF ANBIMA CRI/CRA DOCUMENTS: \n{df_}")
# df_.info()


# cls_ = AnbimaDataCRICRAIndividualCharacteristics(
#     date_ref=None,
#     logger=None,
#     cls_db=None,
#     list_asset_codes=["18L1085826", "19C0000001", "CRA019000GT"],
# )

# df_ = cls_.run()
# print(f"DF ANBIMA CRI/CRA INDIVIDUAL CHARACTERISTICS: \n{df_}")
# df_.info()


# cls_ = AnbimaDataCRICRAPricesFile(
#     date_ref=None,
#     logger=None,
#     cls_db=None,
# )

# df_ = cls_.run()
# print(f"DF ANBIMA CRI/CRA PRICES: \n{df_}")
# df_.info()


# cls_ = AnbimaDataCRICRAPricesWS(
#     date_ref=None,
#     logger=None,
#     cls_db=None,
#     start_page=0,
#     end_page=None,
# )

# df_ = cls_.run()
# print(f"DF ANBIMA CRI/CRA PRICES: \n{df_}")
# df_.info()


# cls_ = AnbimaDataCRICRACharacteristics(
#     date_ref=None,
#     logger=None,
#     cls_db=None,
#     start_page=0,
#     end_page=None,
# )

# df_ = cls_.run()
# print(f"DF ANBIMA CRI/CRA CHARACTERISTICS: \n{df_}")
# df_.info()
