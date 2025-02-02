### GLOBAL CONSTANTS FOR HARDCODES USAGE

import os

from stpstone.opening_config.setup import reading_yaml

# slots of memory to each yaml
PATH = os.path.dirname(os.path.realpath(__file__))
YAML_ANBIMA = reading_yaml(os.path.join(PATH, 'anbima.yaml'))
YAML_B3 = reading_yaml(os.path.join(PATH, 'b3.yaml'))
YAML_BR_MACRO = reading_yaml(os.path.join(PATH, 'br_macro.yaml'))
YAML_BR_TRS = reading_yaml(os.path.join(PATH, 'br_treasury.yaml'))
YAML_CD = reading_yaml(os.path.join(PATH, 'comdinheiro.yaml'))
YAML_GLB_RT = reading_yaml(os.path.join(PATH, 'global_rates.yaml'))
YAML_GEN = reading_yaml(os.path.join(PATH, 'generic.yaml'))
YAML_LLMS = reading_yaml(os.path.join(PATH, 'llms.yaml'))
YAML_MICROSOFT_APPS = reading_yaml(os.path.join(PATH, 'microsoft_apps.yaml'))
YAML_SESSION = reading_yaml(os.path.join(PATH, 'session.yaml'))
YAML_USA_MACRO = reading_yaml(os.path.join(PATH, 'usa_macro.yaml'))
YAML_WGBD = reading_yaml(os.path.join(PATH, 'world_gov_bonds.yaml'))
