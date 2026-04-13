"""Global YAML configuration slots for ingestion modules."""

import os

from stpstone.utils.parsers.yaml import reading_yaml


root_path = os.path.dirname(os.path.realpath(__file__))

# * bylaws
#   BR
YAML_INVESTMENT_FUNDS_BYLAWS = reading_yaml(
    os.path.join(root_path, "countries/br/bylaws/cvm_investment_funds.yaml")
)

YAML_ANBIMA_DATA_API = reading_yaml(
    os.path.join(root_path, "countries/br/registries/anbima_data_api.yaml"))
