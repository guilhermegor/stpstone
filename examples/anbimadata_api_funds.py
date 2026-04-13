"""ANBIMA Data API — funds client usage example.

Requires valid ANBIMA developer credentials. Set the environment variables
``ANBIMA_CLIENT_ID`` and ``ANBIMA_CLIENT_SECRET`` before running.
"""

import os

from stpstone.utils.providers.br.anbimadata_api.anbimadata_api_funds import AnbimaDataFunds


client = AnbimaDataFunds(
	str_client_id=os.environ["ANBIMA_CLIENT_ID"],
	str_client_secret=os.environ["ANBIMA_CLIENT_SECRET"],
	str_env="dev",
)

df_funds = client.funds_trt(int_pg=0)
print(f"Funds catalogue — shape: {df_funds.shape}")
print(df_funds.head())

raw_hist = client.fund_raw(str_code_fnd="123456")
print(f"Raw historical response: {raw_hist}")
