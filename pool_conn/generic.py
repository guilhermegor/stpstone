### GENERIC FUNCTIONS TO HANDLE POOL CONNECTIONS ###

# decorators
def singleton(cls, dict_instances=dict()):
    '''
    DOCSTRING: TRACKS WHETER OR NOT AN INSTANCE OF A CLASS IS ALREADY CREATED, KILLING THE OLD ONE 
        IF SO
    INPUTS: CLASS
    OUTPUTS: INSTANCE
    '''
    def get_instance(*args, **kwargs):
        if cls not in dict_instances:
            dict_instances[cls] = cls(*args, **kwargs)
        return dict_instances[cls]
    return get_instance