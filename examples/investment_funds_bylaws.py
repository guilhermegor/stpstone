import pandas as pd
from getpass import getuser
from time import sleep
from random import shuffle
from stpstone.ingestion.countries.br.bylaws.investment_funds import InvestmentFundsBylawsBR
from stpstone.utils.cals.handling_dates import DatesBR
from stpstone.utils.parsers.lists import HandlingLists


def clean_excel_text(text):
    if not isinstance(text, str):
        return text
    # remove control characters and other problematic chars
    return ''.join(char for char in text if 31 < ord(char) < 127 or char in '\t\n\r')

int_chunk = 200
list_ser = list()

df_ = pd.read_excel("data/input-funds-regex-bylaws.xlsx")
df_['URL_SLUG'] = df_['URL Regulamento'].str.replace(
    'https://web.cvm.gov.br/app/fundosweb/fundos/regulamento/obter/por/arquivo/',
    ''
)
list_slugs = df_['URL_SLUG'].tolist()[:3000]
shuffle(list_slugs)
list_chunks = HandlingLists().chunk_list(list_slugs, None, int_chunk)

for i, list_ in enumerate(list_chunks):
    print(f"{DatesBR().current_timestamp_string} - Processing chunk {i} of {len(list_chunks) - 1}")
    cls_ = InvestmentFundsBylawsBR(list_slugs=list_)
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
    list_ser.extend(df_.to_dict(orient="records"))
    sleep(60)

df_ = pd.DataFrame(list_ser)
df_.to_csv("data/consolidated-investment-funds-bylaws-infos_{}_{}_{}.csv".format(
    getuser(),
    DatesBR().curr_date.strftime('%Y%m%d'),
    DatesBR().curr_time.strftime('%H%M%S')
), index=False)
