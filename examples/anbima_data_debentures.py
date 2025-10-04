"""Example of anbima data debentures."""

from datetime import date
from typing import Union

import pandas as pd

from stpstone.ingestion.countries.br.registries.anbima_data_debentures import (
    AnbimaDataDebenturesAvailable,
    AnbimaDataDebenturesCharacteristics,
    AnbimaDataDebenturesDocuments,
    AnbimaDataDebenturesPrices,
)


cls_ = AnbimaDataDebenturesPrices(
    date_ref=None, 
    logger=None, 
    cls_db=None, 
    debenture_codes=["AALM11", "ABPA11", "AAJR11"],
)

df_ = cls_.run()
print(f"DF ANBIMA DEBENTURES: \n{df_}")
df_.info()


# cls_ = AnbimaDataDebenturesDocuments(
#     date_ref=None, 
#     logger=None, 
#     cls_db=None, 
#     debenture_codes=["AALM11", "ABPA11", "AAJR11"],
# )

# df_ = cls_.run()
# print(f"DF ANBIMA DEBENTURES: \n{df_}")
# df_.info()


# cls_ = AnbimaDataDebenturesCharacteristics(
#     date_ref=None, 
#     logger=None, 
#     cls_db=None, 
#     debenture_codes=["AALM11", "ABPA11", "AAJR11"],
# )
# df_ = cls_.run()
# print(f"DF ANBIMA DEBENTURES: \n{df_}")
# df_.info()


# cls_ = AnbimaDataDebenturesAvailable(
#     date_ref=None, 
#     logger=None, 
#     cls_db=None, 
#     start_page=1, 
#     end_page=5,
# )

# df_ = cls_.run()
# print(f"DF ANBIMA DEBENTURES: \n{df_}")
# df_.info()