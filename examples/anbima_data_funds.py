"""Example usage of AnbimaDataFundsAvailable class."""

from stpstone.ingestion.countries.br.registries.anbima_data_funds import AnbimaDataFundsAvailable


cls_ = AnbimaDataFundsAvailable(
    date_ref=None,
    logger=None, 
    cls_db=None, 
    start_page=0,
    end_page=2,
)

df_ = cls_.run()
print(f"DF ANBIMA DATA FUNDS: \n{df_}")
df_.info()