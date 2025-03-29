import pandas as pd
from getpass import getuser
from stpstone.ingestion.countries.br.registries.anbima_data_funds import FundsDecrypt
from stpstone._config.global_slots import YAML_ANBIMA_DATA_FUNDS
from stpstone.utils.parsers.lists import HandlingLists
from stpstone.utils.cals.handling_dates import DatesBR


df_ = pd.read_csv("data/anbima-avl-funds-cons.csv", sep=",")
list_cod_anbima_checked = df_["COD_ANBIMA"].tolist()

cls_ = FundsDecrypt(YAML_ANBIMA_DATA_FUNDS)
list_range = list(range(42_000, 2_000_000, 1_000))
list_chunks = HandlingLists().chunk_list(list_range, None, 2)
for list_ in list_chunks:
    for str_prefix in ["C", "S"]:
        print(f"{DatesBR().current_timestamp_string} - LOWER BOUND: {list_[0]}, UPPER BOUND: {list_[-1]}, PREFIX: {str_prefix}")
        list_ser = cls_.urls_funds_builder(list_[0], list_[-1], 1, str_prefix, 11, list_cod_anbima_checked)
        if len(list_ser) == 0: continue
        df_ = pd.DataFrame(list_ser)
        df_.to_csv(
            "data/anbima-avl-funds-pg-{}_{}_{}_{}.csv".format(
                1,
                getuser(),
                DatesBR().curr_date.strftime('%Y%m%d'),
                DatesBR().curr_time.strftime('%H%M%S')
            ),
            index=False
        )
