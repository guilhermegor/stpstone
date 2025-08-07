### GENERIC FUNCTIONS TO HANDLE POOL CONNECTIONS ###

from typing import Dict, Any


# decorators
def singleton(cls):
    """
    DOCSTRING: TRACKS WHETER OR NOT AN INSTANCE OF A CLASS IS ALREADY CREATED, KILLING THE OLD ONE
        IF SO
    INPUTS: CLASS
    OUTPUTS: INSTANCE
    """
    dict_instances = dict()
    def get_instance(*args, **kwargs):
        if cls not in dict_instances:
            dict_instances[cls] = cls(*args, **kwargs)
        return dict_instances[cls]
    return get_instance


class GenericSQL:

    def format_query(self, query_path:str, dict_params:Dict[str, Any]):
        """
        DOCSTRING: FORMATS A QUERY WITH F-STRINGS
        INPUTS: QUERY PATH, DICT OF PARAMETERS
        OUTPUTS: FORMATTED QUERY
        """
        with open(query_path, 'r') as query:
            query_read = query.read()
        return query_read.format(**dict_params)
