from getpass import getuser
from stpstone.ingestion.countries.br.registries.mais_retorno_funds import MaisRetornoFunds
from stpstone.utils.cals.handling_dates import DatesBR


cls_ = MaisRetornoFunds(list_slugs=range(1, 10), int_wait_load_seconds=60, 
                        int_delay_seconds=30, bl_save_html=True)

df_ = cls_.source("avl_funds", bl_fetch=True)
print(f"DF MAIS RETORNO FUNDS: \n{df_}")
df_.to_csv("data/mais-retorno-available-funds_{}_{}_{}.csv".format(
    getuser(),
    DatesBR().curr_date.strftime('%Y%m%d'),
    DatesBR().curr_time.strftime('%H%M%S')
), index=False)
df_.info()