from stpstone.ingestion.countries.br.bylaws.investment_funds import InvestmentFundsBylawsBR


cls_ = InvestmentFundsBylawsBR()

df_ = cls_.source("investment_funds_bylaws", bl_fetch=True)
print(f"***Investment Funds Bylaws: \n{df_}")
