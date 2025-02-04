### GLOBAL CONSTANTS ###

# pypi.org libs
import os
# local libs
from stpstone.parsers.yaml import reading_yaml


# base path
root_path = os.path.dirname(os.path.realpath(__file__))
# slots of memory to each yaml
YAML_ANBIMA = reading_yaml(os.path.join(root_path, 'anbima.yaml'))
YAML_B3 = reading_yaml(os.path.join(root_path, 'b3.yaml'))
YAML_BR_TRS = reading_yaml(os.path.join(root_path, 'br_treasury.yaml'))
YAML_CD = reading_yaml(os.path.join(root_path, 'comdinheiro.yaml'))
YAML_GEN = reading_yaml(os.path.join(root_path, 'generic.yaml'))
YAML_WGBD = reading_yaml(os.path.join(root_path, 'world_gov_bonds.yaml'))
YAML_GLB_RT = reading_yaml(os.path.join(root_path, 'global_rates.yaml'))
YAML_USA_MACRO = reading_yaml(os.path.join(root_path, 'usa_macro.yaml'))
YAML_BR_MACRO = reading_yaml(os.path.join(root_path, 'br_macro.yaml'))
YAML_MICROSOFT_APPS = reading_yaml(os.path.join(root_path, 'microsoft_apps.yaml'))
YAML_LLMS = reading_yaml(os.path.join(root_path, 'llms.yaml'))
YAML_SESSION = reading_yaml(os.path.join(root_path, 'session.yaml'))
YAML_RFB = reading_yaml(os.path.join(root_path, 'rfb.yaml'))