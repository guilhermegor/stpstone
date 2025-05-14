import re
import pandas as pd
import numpy as np
from stpstone.ingestion.countries.ww.macroeconomics.world_gov_bonds import WorldGovBonds
from stpstone.utils.cals.handling_dates import DatesBR


pd.set_option("display.max_rows", None)

def move_nan_rows_up(df_):
    if df_ is None:
        return None
        
    # Make a copy of the DataFrame
    df_shifted = df_.copy()
    
    # Get the column range to shift (from COUNTRY_NAME to SPREAD_VS_TNOTE)
    start_col = df_shifted.columns.get_loc('COUNTRY_NAME')
    end_col = df_shifted.columns.get_loc('SPREAD_VS_TNOTE')
    cols_to_shift = df_shifted.columns[start_col:end_col + 1]
    
    # Identify rows where COUNTRY_NAME is NaN
    nan_indices = df_shifted[df_shifted['COUNTRY_NAME'].isna()].index
    
    # For each NaN row found, shift the rows below it up by one position
    for idx in nan_indices:
        # Only process if there are rows below the NaN row
        if idx < len(df_shifted) - 1:
            # Shift the values in the specified columns
            df_shifted.loc[idx, cols_to_shift] = df_shifted.loc[idx + 1, cols_to_shift].values
            # Set the row below to NaN (optional, remove if you want to keep the original values)
            df_shifted.loc[idx + 1, cols_to_shift] = np.nan
    return df_shifted

def adjust_germany_spread(df_):
    if df_ is None:
        return None
        
    df_adjusted = df_.copy()
    germany_mask = df_adjusted['COUNTRY_NAME'] == 'Germany'
    
    if germany_mask.any():
        germany_idx = germany_mask.idxmax()
        spreads = df_adjusted.loc[germany_idx:, 'SPREAD_VS_BUND'].values
        new_spreads = np.concatenate([['0.0 bp'], spreads[:-1]])
        df_adjusted.loc[germany_idx:, 'SPREAD_VS_BUND'] = new_spreads
    
    return df_adjusted

def adjust_usa_spread(df_):
    if df_ is None:
        return None
        
    df_adjusted = df_.copy()
    germany_mask = df_adjusted['COUNTRY_NAME'] == 'United States'
    
    if germany_mask.any():
        germany_idx = germany_mask.idxmax()
        spreads = df_adjusted.loc[germany_idx:, 'SPREAD_VS_TNOTE'].values
        new_spreads = np.concatenate([['0.0 bp'], spreads[:-1]])
        df_adjusted.loc[germany_idx:, 'SPREAD_VS_TNOTE'] = new_spreads
    
    return df_adjusted

def clean_country_name(name):
    """
    Cleans country names from HTML/tab artifacts.
    Example input: "Qatar\t\t\t\t</a>\n\t\t\t</td>"
    Example output: "Qatar"
    """
    if type(name) != str: return None
    cleaned = re.sub(r'[\t\n<\/].*$', '', name)
    cleaned = cleaned.replace(r"\t", " ").strip()
    cleaned = "Hong Kong" if cleaned == "Hong" \
        else "South Africa" if cleaned == "South" \
        else "New Zeland" if cleaned == "New" \
        else "United States" if cleaned == "United" \
        else cleaned
    return cleaned

cls_ = WorldGovBonds()

df_ = cls_.source("sovereign_spreads", bl_fetch=True)
for _ in range(df_.shape[0]):
    df_ = move_nan_rows_up(df_)
df_ = df_[["COUNTRY_NAME", "RATING", "10Y_BOND_YIELD", "BANK_RATE", "SPREAD_VS_BUND", "SPREAD_VS_TNOTE", "SPREAD_VS_BANK_RATE", "FILE_NAME", "URL", "LOG_TIMESTAMP"]]
df_["COUNTRY_NAME"] = df_["COUNTRY_NAME"].apply(clean_country_name)
df_ = adjust_germany_spread(df_)
df_ = adjust_usa_spread(df_)
print(f"DF WORLD GOV BONDS: \n{df_}")
df_.info()

df_.to_csv(rf"data/world_gov_bonds_{DatesBR().curr_date.strftime('%Y%m%d')}.csv", index=False)

# import requests

# url = "https://www.worldgovernmentbonds.com/wp-json/home/v1/main"

# payload = "{\"DUMMY\":null}"
# headers = {
#   'accept': '*/*',
#   'accept-language': 'en-US,en;q=0.9,pt;q=0.8,es;q=0.7',
#   'content-type': 'application/json; charset=UTF-8',
#   'origin': 'https://www.worldgovernmentbonds.com',
#   'priority': 'u=1, i',
#   'referer': 'https://www.worldgovernmentbonds.com/',
#   'sec-ch-ua': '"Chromium";v="136", "Google Chrome";v="136", "Not.A/Brand";v="99"',
#   'sec-ch-ua-mobile': '?0',
#   'sec-ch-ua-platform': '"Linux"',
#   'sec-fetch-dest': 'empty',
#   'sec-fetch-mode': 'cors',
#   'sec-fetch-site': 'same-origin',
#   'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36',
#   'Cookie': '_ga=GA1.1.986309816.1747186644; _ga_K8P6HJT6M9=GS2.1.s1747212162$o3$g1$t1747212175$j0$l0$h0'
# }

# response = requests.request("POST", url, headers=headers, data=payload)

# print(response.json())