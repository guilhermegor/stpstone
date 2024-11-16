### HANDLING DICTIONARIES ISSUES ###

from operator import itemgetter
from itertools import groupby
from functools import cmp_to_key
from collections import defaultdict, Counter, OrderedDict
from heapq import nsmallest, nlargest


class HandlingDicts:

    def min_val_key(self, dict_active):
        '''
        DOCSTRING: MINIMUN VALUE FOR A GIVEN SET OF VALUES IN A DICTIONARY
        INPUTS: ACTIVE DICTIONARY
        OUTPUTS: KEY, VALUE
        '''
        return min(dict_active.items(), key=itemgetter(1))

    def max_val_key(self, dict_active):
        '''
        DOCSTRING: MAXIMUN VALUE FOR A GIVEN SET OF VALUES IN A DICTIONARY
        INPUTS: ACTIVE DICTIONARY
        OUTPUTS: KEY, VALUE
        '''
        return max(dict_active.items(), key=itemgetter(1))

    def merge_n_dicts(self, *dicts, dict_xpt=dict()):
        '''
        DOCSTRING: MERGE DICTIONARIES, FOR PYTHON 3.5+
        INPUTS: DICTIONARIES
        OUTPUTS: DICTIONARY
        '''
        for dict_ in dicts:
            dict_xpt = {**dict_xpt, **dict_}
        return dict_xpt

    def cmp(self, x, y):
        '''
        Replacement for built-in function cmp that was removed in Python 3
        Compare the two objects x and y and return an integer according to
        the outcome. The return value is negative if x < y, zero if x == y
        and strictly positive if x > y.
        https://portingguide.readthedocs.io/en/latest/comparisons.html#the-cmp-function
        '''
        return (x > y) - (x < y)

    def multikeysort(self, items, columns):
        '''
        REFERENCES: https://stackoverflow.com/questions/1143671/how-to-sort-objects-by-multiple-keys-in-python,
            https://stackoverflow.com/questions/28502774/typeerror-cmp-is-an-invalid-keyword-argument-for-this-function
        DOCSTRING: SORT A LIST OF DICTIONARIES
        INPUTS: LIST OF DICTS AND LIST OF COLUMNS, IF THERE IS A NEGATIVE (-) SIGN ON KEY, 
            IT WIL BE ORDERED IN REVERSE
        OUTPUTS: LIST OF DICTIONARIES
        '''
        comparers = [
            ((itemgetter(col[1:].strip()), -1) if col.startswith('-') else (itemgetter(
                col.strip()), 1))
            for col in columns
        ]

        def comparer(left, right):
            comparer_iter = (
                self.cmp(fn(left), fn(right)) * mult
                for fn, mult in comparers
            )
            return next((result for result in comparer_iter if result), 0)
        return sorted(items, key=cmp_to_key(comparer))

    def merge_dicts(self, list_dicts, list_keys_merge=None, bl_sum_values_key=True):
        '''
        DOCSTRING: MERGE DICTS FOR EVERY KEY REPETITION
        INPUTS: FOREIGNER KEY, DICTS
        OUTPUTS: DICTIONARY
        '''
        # setting default variables
        dict_export = defaultdict(list)
        list_counter_dicts = list()
        # if list of keys to merge is none, return a list of every values for the same key
        if list_keys_merge != None:
            # iterating through dictionaries of interest an merging accordingly to foreigner key
            for dict_ in list_dicts:
                for key, value in dict_.items():
                    if key in list_keys_merge:
                        dict_export[key].append(value)
                    else:
                        dict_export[key] = value
            if bl_sum_values_key == True:
                return {k: (sum(v) if isinstance(v, list) else v) for k, v in dict_export.items()}
            else:
                return dict_export
        else:
            for dict_ in list_dicts:
                list_counter_dicts.append(Counter(dict_))
            return dict(sum(list_counter_dicts))

    def filter_list_dicts(self, list_dicts, foreigner_key, k_value):
        '''
        DOCSTRING: FILTER LIST OF DICTIONARIES
        INPUTS: LIST OF DICTS, FOREINGER KEY, VALUE OF FOREIGNER KEY OF INTEREST
        OUTPUTS: LIST OF DICTS
        '''
        return [dict_ for dict_ in list_dicts if dict_[foreigner_key] == k_value]

    def merge_values_foreigner_keys(self, list_dicts, foreigner_key, list_keys_merge_dict):
        '''
        REFERECES: https://stackoverflow.com/questions/50167565/python-how-to-merge-dict-in-list-of-dicts-based-on-value
        DOCSTRING: MERGE DICTS ACCORDINGLY TO A FOREIGNER KEY IN THE LIST OF DICTS
        INPUTS: LIST OF DICTS, FOREIGNER KEY AND LIST OF KEYS TO MERGE IN A GIVEN SET O DICTS
        OUTPUTS: LIST OF DICTS
        '''
        # setting default variables
        list_dicts_export = list()
        list_foreinger_keys = list()
        list_dicts_export = list()
        # get values from foreinger key
        list_foreinger_keys = list(
            set([dict_[foreigner_key] for dict_ in list_dicts]))
        # iterating through list of foreigner key and merging values of interest
        for key in list_foreinger_keys:
            # filter dicts for the given foreinger key
            list_filtered_dicts = self.filter_list_dicts(
                list_dicts, foreigner_key, key)
            # merge dictionaries accordingly to given keys
            list_dicts_export.append(self.merge_dicts(
                list_filtered_dicts, list_keys_merge_dict))
        # return final result
        return list_dicts_export

    def n_smallest(self, list_dict, key_, n):
        '''
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        '''
        return nsmallest(n, list_dict, key=lambda dict_: dict_[key_])

    def n_largest(self, list_dict, key_, n):
        '''
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        '''
        return nlargest(n, list_dict, key=lambda dict_: dict_[key_])

    def order_dict(self, dict_):
        '''
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        '''
        return OrderedDict(sorted(dict_.items()))

    def group_by_dicts(self, list_dict):
        '''
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        '''
        # sort by the desired field first
        list_dict.sort(key=itemgetter('date'))
        # return iteration in groups
        return groupby(list_dict, key=itemgetter('date'))

    def add_k_v_serialized_list(self, list_dicts, k, v):
        '''
        DOCSTRING: ADD KEY AND VALUE TO EVERY LIST WITHIN A SERIALIZED LIST
        INPUTS:
        OUTPUTS:
        '''
        return [dict(dict_, **{str(k): str(v)}) for dict_ in list_dicts]
    
    def pair_headers_with_data(self, list_headers, list_data, list_dicts=list()):
        '''
        DOCSTRING: PAIR HEADERS AND DATA AS KEYS AND VALUES IN A SERIALIZED LIST
        INPUTS: LIST HEADERS, LIST DATA
        OUTPUTS: LIST
        '''
        # ensuring the list_data length is a multiple of list_headers length
        if len(list_data) % len(list_headers) != 0:
            raise ValueError(
                'The length of list_data is not a multiple of the length of list_headers.')
        # iterate over the list_data in chunks equal to the length of list_headers
        for i in range(0, len(list_data), len(list_headers)):
            # create a dictionary for each chunk
            entry = {list_headers[j]: list_data[i + j] for j in range(len(list_headers))}
            list_dicts.append(entry)
        # returning list of dictionaries
        return list_dicts
