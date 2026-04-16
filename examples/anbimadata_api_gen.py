"""ANBIMA Data API — base client usage example.

Requires valid ANBIMA developer credentials. Set the environment variables
``ANBIMA_CLIENT_ID`` and ``ANBIMA_CLIENT_SECRET`` before running.
"""

import os

from stpstone.utils.providers.br.anbimadata_api.anbimadata_api_gen import AnbimaDataGen


client = AnbimaDataGen(
	str_client_id=os.environ["ANBIMA_CLIENT_ID"],
	str_client_secret=os.environ["ANBIMA_CLIENT_SECRET"],
	str_env="dev",
)

token_info = client.access_token()
print(f"Access token: {token_info['access_token'][:10]}…")

raw = client.generic_request(
	str_app="feed/fundos/v2/fundos?size=5&page=0",
	str_method="GET",
)
print(f"First page response: {raw}")
