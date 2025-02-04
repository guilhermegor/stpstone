### GLOBAL CONSTANTS ###

# pypi.org libs
import os
# local libs
from stpstone.parsers.yaml import reading_yaml


# base path
path_base = os.path.dirname(os.path.realpath(__file__))
# slots of memory to each yaml
YAML_ANBIMA = reading_yaml(os.path.join(path_base, 'anbima.yaml'))
YAML_B3 = reading_yaml(os.path.join(path_base, 'b3.yaml'))
YAML_BR_TRS = reading_yaml(os.path.join(path_base, 'br_treasury.yaml'))
YAML_CD = reading_yaml(os.path.join(path_base, 'comdinheiro.yaml'))
YAML_GEN = reading_yaml(os.path.join(path_base, 'generic.yaml'))
YAML_WGBD = reading_yaml(os.path.join(path_base, 'world_gov_bonds.yaml'))
YAML_GLB_RT = reading_yaml(os.path.join(path_base, 'global_rates.yaml'))
YAML_USA_MACRO = reading_yaml(os.path.join(path_base, 'usa_macro.yaml'))
YAML_BR_MACRO = reading_yaml(os.path.join(path_base, 'br_macro.yaml'))
YAML_MICROSOFT_APPS = reading_yaml(os.path.join(path_base, 'microsoft_apps.yaml'))
YAML_LLMS = reading_yaml(os.path.join(path_base, 'llms.yaml'))
YAML_SESSION = reading_yaml(os.path.join(path_base, 'session.yaml'))
YAML_RFB = reading_yaml(os.path.join(path_base, 'rfb.yaml'))