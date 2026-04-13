"""INOA Alpha Tools API — usage example.

Requires valid INOA Alpha Tools credentials. Set the environment variables
``INOA_USER``, ``INOA_PASSWORD``, ``INOA_HOST``, and ``INOA_INSTANCE``
before running.
"""

from datetime import datetime
import os

from stpstone.utils.providers.br.inoa.inoa import AlphaTools


client = AlphaTools(
	str_user=os.environ["INOA_USER"],
	str_passw=os.environ["INOA_PASSWORD"],
	str_host=os.environ["INOA_HOST"],
	str_instance=os.environ["INOA_INSTANCE"],
	date_start=datetime(2024, 1, 1),
	date_end=datetime(2024, 1, 31),
)

df_funds = client.funds
print(f"Funds retrieved: {len(df_funds)} rows")
print(df_funds.head())

if not df_funds.empty:
	list_ids = df_funds["ID"].tolist()[:5]
	df_quotes = client.quotes(list_ids)
	print(f"Quotes retrieved: {len(df_quotes)} rows")
	print(df_quotes.head())
