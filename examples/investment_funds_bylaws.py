from getpass import getuser
from stpstone.ingestion.countries.br.bylaws.investment_funds import InvestmentFundsBylawsBR
from stpstone.utils.cals.handling_dates import DatesBR

def clean_excel_text(text):
    if not isinstance(text, str):
        return text
    # remove control characters and other problematic chars
    return ''.join(char for char in text if 31 < ord(char) < 127 or char in '\t\n\r')

cls_ = InvestmentFundsBylawsBR()
df_ = cls_.source("investment_funds_bylaws", bl_fetch=True)

# clean all string columns
for col in df_.select_dtypes(include=['object']):
    df_[col] = df_[col].apply(clean_excel_text)

# save with openpyxl engine which is more tolerant
output_path = "data/investment-funds-bylaws-infos_{}_{}_{}.xlsx".format(
    getuser(),
    DatesBR().curr_date.strftime('%Y%m%d'),
    DatesBR().curr_time.strftime('%H%M%S'))

df_.to_excel(output_path, engine='openpyxl', index=False)
