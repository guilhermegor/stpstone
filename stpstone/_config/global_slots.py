"""Global YAML configuration slots for ingestion modules."""

import os

from stpstone.utils.parsers.yaml import reading_yaml


root_path = os.path.dirname(os.path.realpath(__file__))

YAML_ANBIMA_DATA_API = reading_yaml(
    os.path.join(root_path, "countries/br/registries/anbima_data_api.yaml"))
