### HANDLING LISTS ###

import re
import bisect
import numpy as np
from collections import OrderedDict, Counter
from itertools import chain, tee, product
from numbers import Number
from typing import List, Any, Iterable, Tuple, Dict
from logging import Logger
from typing import List, Optional
from stpstone.utils.parsers.json import JsonFiles
from stpstone.utils.parsers.str import StrHandler
from stpstone.utils.parsers.numbers import NumHandler
from stpstone.utils.loggs.create_logs import CreateLog


class ListHandler:

    def get_first_occurrence_within_list(self, list_, obj_occurrence=None, bl_uppercase=False,
                                         bl_last_uppercase_before_capitalized=False, int_error=-1,
                                         int_error_obj_occurrence=-2, bl_regex_alphanumeric_false=False,
                                         bl_ignore_sole_letter=True,
                                         str_original_replace_1=',',
                                         str_original_replace_2='.', str_result_replace=''):
        if bl_uppercase == True:
            try:
                return list_.index(next(obj for obj in list_ if obj.isupper() == True))
            except StopIteration:
                return int_error
        elif obj_occurrence != None:
            for el in list_:
                if StrHandler().match_string_like(
                        StrHandler().remove_diacritics(el),
                        StrHandler().remove_diacritics(obj_occurrence)):
                    return list_.index(el)
            else:
                return int_error_obj_occurrence
        elif bl_last_uppercase_before_capitalized == True:
            if (StrHandler().is_capitalized(StrHandler().remove_diacritics(
                list_[0])) == True) or (StrHandler().remove_diacritics(
                    list_[0]).islower() == True):
                return int_error
            for i in range(len(list_) - 2):
                if (list_[i].replace(',', '').isupper() == True) and ((
                        StrHandler().is_capitalized(list_[i + 1].replace(',', '')) == True) or
                        (list_[i + 1].islower() == True)):
                    if bl_ignore_sole_letter == True:
                        if len(StrHandler().remove_non_alphanumeric_chars(list_[i])) == 1:
                            return i - 1
                        else:
                            return i
                    else:
                        return i
            else:
                return False
        # find first error to regex alphanumeric within a list
        elif bl_regex_alphanumeric_false == True:
            for i in range(len(list_)):
                if StrHandler().regex_match_alphanumeric(StrHandler().remove_diacritics(
                    list_[i].replace(
                        str_original_replace_1, str_result_replace).replace(
                            str_original_replace_2, str_result_replace)).strip()) is None:
                    return i
            else:
                return int_error
        else:
            raise Exception(
                'Neither boolean uppercase, nor object occurrence, were searched '
                + 'within the list for the first manifestation, please revisit the inputs')

    def get_list_until_invalid_occurrences(self, list_: List[Any], list_invalid_values: List[Any]) \
        -> List[Any]:
        # remove diacritcs from both lists
        list_ = [StrHandler().remove_diacritics(el) for el in list_]
        list_invalid_values = [StrHandler().remove_diacritics(el) for el in
                               list_invalid_values]
        # setting initial variables
        list_export = list()
        # looping through each element to find the first occurrence, and then break the loop
        for el in list_:
            if any([StrHandler().match_string_like(el, str_) == True for str_
                    in list_invalid_values]):
                break
            else:
                list_export.append(el)
        return list_export

    def first_numeric(self, list_):
        try:
            return next(iter([el for el in list_ if str(el).isnumeric()]))
        except StopIteration:
            return False

    def get_lower_upper_bound(self, sorted_list: List[Number], value_to_put_in_between: Number) \
        -> Dict[str, Number]:
        """
        Get lower and upper bound of data that a value is in between

        Args:
            sorted_list (list): List in ascending order
            value_to_put_in_between (int): Value to put in between

        Notes:
            https://stackoverflow.com/questions/55895500/need-to-check-if-value-is-between-two-numbers-in-a-list
        """
        # list index for for lower and upper bound
        if (value_to_put_in_between in sorted_list) and sorted_list[-1] != value_to_put_in_between:
            list_idx_lower_upper_bound = [bisect.bisect_left(sorted_list, value_to_put_in_between),
                                          bisect.bisect_left(sorted_list, value_to_put_in_between) + 1]
        else:
            list_idx_lower_upper_bound = [bisect.bisect_left(sorted_list, value_to_put_in_between) - 1,
                                          bisect.bisect_left(sorted_list, value_to_put_in_between)]
        # dictionary with responses
        if all(0 <= i <= len(sorted_list) for i in list_idx_lower_upper_bound):
            return {
                'lower_bound': sorted_list[list_idx_lower_upper_bound[0]],
                'upper_bound': sorted_list[list_idx_lower_upper_bound[1]]
            }
        else:
            raise Exception('{} value is outside the bounds of {}'.format(
                value_to_put_in_between, sorted_list))

    def get_lower_mid_upper_bound(self, sorted_list: List[Number], value_to_put_in_between: Number) \
        -> Dict[str, Number]:
        """
        DOCSTRING: LOWER, MIDDLE AND UPPER BOUND OF DATA THAT A VALUE IS IN BETWEEN; IT CONSIDERS A
            LIST IN ASCENDING ORDER
        INPUTS: SORTED LIST AND VALUE TO BE IN BETWEEN OF DATA WITHIN THE LIST
        OUTPUTS JSON WITH LOWER, MIDDLE, UPPER BOUND AND BOOLEAN WITH END OF LIST
        """
        # list index for for lower and upper bound
        if (value_to_put_in_between in sorted_list) and sorted_list[-1] != value_to_put_in_between:
            list_idx_lower_upper_bound = [bisect.bisect_left(sorted_list, value_to_put_in_between),
                                          bisect.bisect_left(sorted_list,
                                                             value_to_put_in_between) + 1]
        else:
            list_idx_lower_upper_bound = [bisect.bisect_left(sorted_list,
                                                             value_to_put_in_between) - 1,
                                          bisect.bisect_left(sorted_list, value_to_put_in_between)]
        # dictionary with responses
        if all(0 <= i <= len(sorted_list) for i in list_idx_lower_upper_bound) and \
                len(sorted_list) > 2:
            try:
                dict_message = {
                    'lower_bound': sorted_list[list_idx_lower_upper_bound[0]],
                    'middle_bound': sorted_list[list_idx_lower_upper_bound[1]],
                    'upper_bound': sorted_list[list_idx_lower_upper_bound[1] + 1],
                    'end_of_list': False
                }
            except:
                dict_message = {
                    'lower_bound': sorted_list[list_idx_lower_upper_bound[0] - 1],
                    'middle_bound': sorted_list[list_idx_lower_upper_bound[0]],
                    'upper_bound': sorted_list[list_idx_lower_upper_bound[1]],
                    'end_of_list': True
                }
            return JsonFiles().send_json(dict_message)
        else:
            raise Exception('{} value is outside the bounds of {}'.format(
                value_to_put_in_between, sorted_list))

    def closest_bound(self, sorted_list: List[Number], value_to_put_in_between: Number) \
        -> List[Number]:
        """
        DOCSTRING: CLOSEST BOUND TO A VALUE IN A LIST
        INPUTS: SORTED LIST, VALUE TO PUT IN BETWEEN
        OUTPUTS: VALUE
        """
        return sorted_list[min(range(len(sorted_list)), key=lambda i:
                               abs(sorted_list[i] - value_to_put_in_between))]

    def closest_number_within_list(self, list_: List[Number], number: Number) -> List[Number]:
        """
        DOCSTRING: CLOSEST NUMBER TO NUMBER_ WITHIN A LIST
        INPUTS: LIST OF NUMBERS (NOT NECESSARILY SORTED) AND NUMBER K
        OUTPUTS: FLOAT/INTEGER
        """
        return list_[min(range(len(list_)), key=lambda i: abs(list_[i]- number))]

    def first_occurrence_like(self, list_, str_like):
        """
        DOCSTRING: FIRST OCCURRENCE OF A MATCHING STRING WITHIN A LIST
        INPUTS: LIST AND STRING LIKE
        OUTPUTS: INTEGER
        """
        return list_.index(next(x for x in list_ if StrHandler().match_string_like(
            x, str_like) == True))

    def remove_duplicates(self, list_interest):
        """
        DOCSTRING: REMOVING DUPLICATES FROM A GIVEN LIST
        INPUTS: LIST
        OUTPUTS: LIST WITHOUT DUPLICATES
        """
        return list(OrderedDict.fromkeys(list_interest))

    def nth_smallest_numbers(self, list_numbers, nth_smallest):
        """
        DOCSTRING: RETURN THE NTH-SMALLEST NUMBERS FROM A LIST
        INPUTS: LIST NUMBERS
        OUTPUTS: NUMPY ARRAY
        """
        # turning into a array
        array_numbers = np.array(list_numbers)
        # sort array
        array_numbers = np.sort(array_numbers)
        # returning the nth-smallest numnbers
        return array_numbers[0:nth_smallest]

    def extend_lists(self, *lists, bl_remove_duplicates=True):
        """
        DOCSTRING: EXTEND N-LISTS AND REMOVE ITS DUPLICATES
        INPUTS: *ARGS WITH N-LISTS
        OUTPUTS: LIST
        """
        # returning list with n-lists to append and remove duplicates
        list_extended_lists = list()
        # iterating through each list and appending to the final one
        for list_ in lists:
            list_extended_lists = chain(list_extended_lists, list_)
        # removing duplicates
        if bl_remove_duplicates == True:
            list_extended_lists = ListHandler().remove_duplicates(list_extended_lists)
        else:
            list_extended_lists = list(list_extended_lists)
        # returning final list
        return list_extended_lists

    def chunk_list(self, list_to_chunk, str_character_divides_clients=' ',
                   int_chunk=150, bl_remove_duplicates=True):
        """
        DOCSTRING: LIST TO CHUNK IN THE LIMIT SIZE
        INPUTS: LIST TO CHUNK, STRING CHARACTER TO DIVIDE CLIENT (DEFAULT), AND CHUNK (DEFAULT)
        OUTPUTS: LIST
        """
        # setting variables
        list_chunked = list()
        # remove duplicates if is user's will
        if bl_remove_duplicates == True:
            list_to_chunk = ListHandler().remove_duplicates(list_to_chunk)
        # creating chunks positions
        list_position_chunks = NumHandler().multiples(int_chunk, len(list_to_chunk))
        inf_limit = list_position_chunks[0]
        sup_limit = list_position_chunks[1]
        # checking wheter str_character_divides_clients is None, in this case append lists
        if str_character_divides_clients is None:
            return [list_to_chunk[x: x + int_chunk] for x in range(0, len(list_to_chunk), int_chunk)]
        # iterating through list to chunk, dividing in sublists with maximum size
        if len(list_position_chunks) > 2:
            for lim in list_position_chunks[2:]:
                #   narrowing query list
                if str_character_divides_clients is None:
                    list_chunked.append(list_to_chunk[inf_limit: sup_limit])
                else:
                    list_chunked.append(str_character_divides_clients.join(
                        list_to_chunk[inf_limit: sup_limit]))
                #   cutting limits
                inf_limit = sup_limit
                sup_limit = lim
            #   last append of sublist
            if str_character_divides_clients is None:
                list_chunked.append(list_to_chunk[
                    list_position_chunks[-2]: list_position_chunks[-1]])
            else:
                list_chunked.append(str_character_divides_clients.join(list_to_chunk[
                    list_position_chunks[-2]: list_position_chunks[-1]]))
        else:
            #   append list, if the size is inferior to chunk
            list_chunked.append(str_character_divides_clients.join(list_to_chunk[
                inf_limit: sup_limit]))
        #   removing duplicates
        list_chunked = ListHandler().remove_duplicates(list_chunked)
        #   returning final result
        return list_chunked

    def cartesian_product(self, list_lists, int_break_n_n=None):
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        # setting variables
        list_export = list()
        # list of cartesian product of lists
        list_cartesian_product = list(product(*list_lists))
        # iterating through cartesian products of lists, if break in max values for tuples is different
        #   from none
        if int_break_n_n != None:
            for tup in list_cartesian_product:
                if (tup[:int_break_n_n] not in list_export) and (all([tup[:int_break_n_n][i] != tup[
                        :int_break_n_n][i - 1] for i in range(1, len(tup[:int_break_n_n]))])):
                    list_export.append(tup[:int_break_n_n])
            return list_export
        else:
            return list_cartesian_product

    def sort_alphanumeric(self, list_):
        """
        REFERENCES: https://stackoverflow.com/questions/2669059/how-to-sort-alpha-numeric-set-in-python
        DOCSTRING: SORT ALPHANUMERIC DATA FROM LIST
        INPUTS:
        OUTPUTS:
        """
        def convert(text): return int(text) if text.isdigit() else text
        def alphanum_key(key): return [convert(c)
                                       for c in re.split('([0-9]+)', key)]
        return sorted(list_, key=alphanum_key)

    def pairwise(self, iterable: Iterable) -> List[Tuple[Any, Any]]:
        """
        Return successive overlapping pairs taken from the input iterable

        Args:
            iterable (iterable): Input iterable

        Notes:
            https://docs.python.org/3/library/itertools.html#itertools.pairwise
        """
        a, b = tee(iterable)
        next(b, None)
        return list(zip(a, b))

    def discard_from_list(self, list_: List, list_items_remove: List[Any]) -> List[Any]:
        for item in list_items_remove:
            if item in list_:
                list_.remove(item)
        return list_

    def absolute_frequency(self, list_):
        return Counter(list_)

    def flatten_list(self, list_):
        return [x for xs in list_ for x in xs]

    def remove_consecutive_duplicates(self, list_: List[Any]) -> List[Any]:
        list_xpt = [list_[0]]
        for i in range(1, len(list_)):
            if list_[i] != list_[i - 1]:
                list_xpt.append(list_[i])
        return list_xpt

    def replace_first_occurrence(self, list_: List[str], str_old_value: str, str_new_value: str, 
                                 logger: Optional[Logger] = None) -> List[str]:
        if str_old_value in list_:
            index = list_.index(str_old_value)
            list_[index] = str_new_value
            # print(f"Value {str_old_value} replaced by {list_[index]} / Index: {index}")
        else:
            CreateLog().log_message(logger, f"Value {str_old_value} not found in list", "warning")
        return list_

    def replace_last_occurrence(self, list_: List[str], str_old_value: str, str_new_value: str, 
                                 logger: Optional[Logger] = None) -> List[str]:
        if str_old_value in list_:
            index = list_[::-1].index(str_old_value)
            list_[index] = str_new_value
            # print(f"Value {str_old_value} replaced by {list_[index]} / Index: {index}")
        else:
            CreateLog().log_message(logger, f"Value {str_old_value} not found in list", "warning")
        return list_