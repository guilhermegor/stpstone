import os
from dotenv import load_dotenv
from stpstone.ingestion.countries.br.registries.anbima_data_funds import FundsConsolidated
from stpstone.utils.connections.clouds.minio import MinioClient
from stpstone.utils.parsers.folders import DirFilesManagement
from stpstone._config.global_slots import YAML_ANBIMA_DATA_FUNDS


path_project = DirFilesManagement().find_project_root()
path_env = f"{path_project}/.env"

load_dotenv(path_env)

client_minio = MinioClient(os.getenv("MINIO_USER"), os.getenv("MINIO_PASSWORD"),
                           "127.0.0.1:9000", bl_secure=False)
cls_funds_cons = FundsConsolidated(
    client_minio, "html", r"//script[contains(text(), 'self.__next_f.push')]/text()",
    YAML_ANBIMA_DATA_FUNDS["trt_indicadores_fundos"]["re_patterns"]
)
df_ = cls_funds_cons.funds_infos_ts
print(f"DF FUNDS CONSOLIDATED - ANBIMA DATA: n{df_}")
