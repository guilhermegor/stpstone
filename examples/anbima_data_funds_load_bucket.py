import pandas as pd
from stpstone._config.global_slots import YAML_ANBIMA_DATA_FUNDS
from stpstone.utils.connections.clouds.minio import MinioClient
from stpstone.ingestion.countries.br.registries.anbima_data_funds import FundsFetcher


minio_client = MinioClient("minioadmin", "minioadmin", "127.0.0.1:9000", bl_secure=False)

file_anbima_funds = "data/anbima-avl-funds-cons.csv"
reader = pd.read_csv(file_anbima_funds, sep=",")
df_ = pd.DataFrame(reader)
df_ = df_.astype({
    'COD_ANBIMA': str,
    'URL_ANBIMA_FUND': str,
    'STATUS_CODE': int
})
print(f'SHAPE DF: {df_.shape}')
list_slugs = df_[df_["STATUS_CODE"] != 200]["COD_ANBIMA"].tolist()
print(f'LEN LIST SLUGS: {len(list_slugs)}')

cls_ = FundsFetcher(
    "raw_indicadores_fundos",
    YAML_ANBIMA_DATA_FUNDS,
    list_slugs,
    "html",
    None,
    minio_client
).store_s3_data
