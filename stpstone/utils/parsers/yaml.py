### PARSER - YAML ###

import yaml


def reading_yaml(yaml_path):
    return yaml.load(open(yaml_path), Loader=yaml.FullLoader)
