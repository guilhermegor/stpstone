"""Example usage of AnbimaDataFundsAvailable class."""

from stpstone.ingestion.countries.br.registries.anbima_data_funds import (
    AnbimaDataFundsAbout,
    AnbimaDataFundsAvailable,
)


# cls_ = AnbimaDataFundsAvailable(
#     date_ref=None,
#     logger=None, 
#     cls_db=None, 
#     start_page=0,
#     end_page=2,
# )

# df_ = cls_.run()
# print(f"DF ANBIMA DATA FUNDS: \n{df_}")
# df_.info()


cls_ = AnbimaDataFundsAbout(
    date_ref=None,
    logger=None, 
    cls_db=None, 
    # list_fund_codes=["S0000634344", "C0000699136", "S0000634336", "S0000602205"],
    list_fund_codes=["S0000634344"],
)

dict_ = cls_.run()
df_characteristics, df_related, df_about = \
    dict_["characteristics"], \
    dict_["related"], \
    dict_["about"]

print(f"DF ANBIMA DATA FUNDS CHARACTERISTICS: \n{df_characteristics}")
df_characteristics.info()

print(f"DF ANBIMA DATA FUNDS RELATED: \n{df_related}")
df_related.info()

print(f"DF ANBIMA DATA FUNDS ABOUT: \n{df_about}")
df_about.info()