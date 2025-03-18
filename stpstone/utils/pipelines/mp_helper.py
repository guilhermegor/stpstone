### MULTIPROCESSING HELPER ###

import multiprocessing as mp
from typing import Callable, Optional


def mp_worker(args):
    """
    REFERENCES: https://chatgpt.com/share/6737ddcc-8564-800c-908f-9e36c311a834
    DOCSTRING: GENERALIZED WORKER FUNCTION TO CALL ANY METHOD OR FUNCTION WITH ITS ARGUMENTS
    INPUTS: ARGS (TUPLE) CONTAINING CALLABLE OBJECT, POSITIONAL ARGUMENTS AND KEYWORD ARGUMENTS:
        - CALLABLE: THE FUNCTION OR METHOD TO BE CALLED
        - POSITIONAL_ARGUMENTS: THE ARGUMENTS TO BE PASSED TO THE FUNCTION OR METHOD
        - KEYWORD_ARGUMENTS: A DICTIONARY OF KEYWORD ARGUMENTS
    OUTPUTS: THE RESULT OF THE CALLABLE
    """
    func, positional_args, keyword_args = args
    return func(*positional_args, **keyword_args)

def mp_run_parallel(
    func:Callable,
    list_task_args:Optional[list]=None,
    int_ncpus:int=mp.cpu_count() - 2 if mp.cpu_count() > 2 else 1
):
    """
    REFERENCES: https://chatgpt.com/share/6737ddcc-8564-800c-908f-9e36c311a834
    DOCSTRING: RUN WORKER PARALLIZED WITH MULTIPROCESSING, IN ORDER TO HANDLE THE PICKLING
        REQUIREMENT OF MULTIPROCESSING:
        - THE MP RELIES ON PICKLING TO SERIALIZE OBJECTS WHEN SENDING THEM TO WORKER PROCESSES
        - INSTANCE METHODS ARE NOT PICKABLE BY DEFAULT, IN ORDER TO HELP THIS A WORKER IS DEFINED
        - OBS.: ESPECIALLY ON WINDOWS, IT IS ESSENTIAL TO PROTECT THE ENTRY POINT OF THE PROGRAM
            USING IF __NAME__ == '__MAIN__' TO PREVENT RECURSIVE PROCESS SPAWNING
    INPUTS: LIST ARGS (LIST OF TUPLES, BEING THE FIRST VALUE SELF, IN CASE OF A CLASS INSTANCE),
        WORKER, INT NCPUS
    OUTPUTS: LIST
    """
    if list_task_args is None:
        list_task_args = [((), {})]
    # prepare arguments for the worker
    args_list = [(func, pos_args, kw_args) for pos_args, kw_args in list_task_args]
    # use multiprocessing Pool for parallel processing
    with mp.Pool(processes=int_ncpus) as pool:
        list_results = pool.map(mp_worker, args_list)
    # returning list of results
    return list_results
