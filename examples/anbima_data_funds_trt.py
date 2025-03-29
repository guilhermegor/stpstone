import os
from dotenv import load_dotenv
from getpass import getuser
from stpstone.ingestion.countries.br.registries.anbima_data_funds import FundsConsolidated
from stpstone.utils.connections.clouds.minio import MinioClient
from stpstone.utils.parsers.folders import DirFilesManagement
from stpstone._config.global_slots import YAML_ANBIMA_DATA_FUNDS
from stpstone.utils.cals.handling_dates import DatesBR


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
path_orig_csv = "data/anbima-data-funds-trt_{}_{}_{}.csv".format(
    getuser(),
    DatesBR().curr_date.strftime('%Y%m%d'),
    DatesBR().curr_time.strftime('%H%M%S')
)
path_destination_csv = r"C:\Users\guiro\OneDrive\Workspace\BASES\anbima-data-funds-trt_{}_{}_{}.csv".format(
    getuser(),
    DatesBR().curr_date.strftime('%Y%m%d'),
    DatesBR().curr_time.strftime('%H%M%S')
)
df_.to_csv(
    path_orig_csv,
    index=False
)

_ = DirFilesManagement().copy_file(path_orig_csv, path_destination_csv)
