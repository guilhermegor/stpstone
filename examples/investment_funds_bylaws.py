from getpass import getuser
from stpstone.ingestion.countries.br.bylaws.investment_funds import InvestmentFundsBylawsBR
from stpstone.utils.cals.handling_dates import DatesBR


cls_ = InvestmentFundsBylawsBR()

df_ = cls_.source("investment_funds_bylaws", bl_fetch=True)
df_.to_csv("data/investment-funds-bylaws-infos_{}_{}_{}.csv".format(
    getuser(),
    DatesBR().curr_date.strftime('%Y%m%d'),
    DatesBR().curr_time.strftime('%H%M%S')),
    index=False
)
print(f"***Investment Funds Bylaws: \n{df_}")
