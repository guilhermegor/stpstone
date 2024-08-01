### GLOBAL CONSTANTS FOR HARDCODES USAGE

import os
from stpstone.opening_config.setup import reading_yaml

# slots of memory to each yaml
YAML_ANBIMA = reading_yaml(r'{}\anbima.yaml'.format(
    os.path.dirname(os.path.realpath(__file__))))
YAML_B3 = reading_yaml(r'{}\b3.yaml'.format(
    os.path.dirname(os.path.realpath(__file__))))
YAML_BR_TRS = reading_yaml(r'{}\br_treasury.yaml'.format(
    os.path.dirname(os.path.realpath(__file__))))
YAML_CD = reading_yaml(r'{}\comdinheiro.yaml'.format(
    os.path.dirname(os.path.realpath(__file__))))
YAML_GEN = reading_yaml(r'{}\general.yaml'.format(
    os.path.dirname(os.path.realpath(__file__))))
YAML_WGBD = reading_yaml(r'{}\world_gov_bonds.yaml'.format(
    os.path.dirname(os.path.realpath(__file__))))
YAML_GLB_RT = reading_yaml(r'{}\global_rates.yaml'.format(
    os.path.dirname(os.path.realpath(__file__))))
YAML_USA_MACRO = reading_yaml(r'{}\usa_macro.yaml'.format(
    os.path.dirname(os.path.realpath(__file__))))
YAML_BR_MACRO = reading_yaml(r'{}\br_macro.yaml'.format(
    os.path.dirname(os.path.realpath(__file__))))