### HANDLING LISTS ###

import re
import bisect
import numpy as np
from heapq import heappush, heappop
from collections import OrderedDict, Counter
from itertools import chain, tee, product
from stpstone.parsers.json import JsonFiles
from stpstone.parsers.str import StrHandler
from stpstone.parsers.numbers import NumHandler


class HandlingLists:

    def get_first_occurrence_within_list(self, list_, obj_occurrence=None, bl_uppercase=False,
                                         bl_last_uppercase_before_capitalized=False, int_error=-1,
                                         int_error_obj_occurrence=-2, bl_regex_alphanumeric_false=False,
                                         bl_ignore_sole_letter=True,
                                         str_original_replace_1=',',
                                         str_original_replace_2='.', str_result_replace='',
                                         bl_audit=False):
        """
        DOCSTRING: GET THE FIRST OCCURRENCE OF AN OBJECT WITHIN A LIST
        INPUTS: LIST, OBJECT AND BOOLEAN UPPERCASE (FALSE AS DEFAULT)
        OUTPUTS: INTEGER, FALSE IF THERE IS NO OCCURRENCE
        """
        # first occurrence of uppercase
        if bl_uppercase == True:
            try:
                return list_.index(next(obj for obj in list_ if obj.isupper() == True))
            except StopIteration:
                return int_error
        # first occurrence of a given object
        elif obj_occurrence != None:
            for el in list_:
                if bl_audit == True:
                    print('BOOLEAN - MATCH STRING: {} {} {}'.format(
                        StrHandler().remove_diacritics(el),
                        StrHandler().remove_diacritics(obj_occurrence),
                        StrHandler().match_string_like(
                            StrHandler().remove_diacritics(el),
                            StrHandler().remove_diacritics(obj_occurrence))
                    ))
                if StrHandler().match_string_like(
                        StrHandler().remove_diacritics(el),
                        StrHandler().remove_diacritics(obj_occurrence)):
                    if bl_audit == True:
                        print('LIST INDEX: {}'.format(
                            list_.index(el)
                        ))
                    return list_.index(el)
            else:
                return int_error_obj_occurrence
        # last occurrence of an uppercase before a capitalized one
        elif bl_last_uppercase_before_capitalized == True:
            #   checking wheter the first occurrence is a capitalize or lower letter, in this
            #       case return the integer error - in this case return the string with no
            #       trims
            if bl_audit == True:
                print('RECEIVED LIST: {}'.format(list_))
                print('FIRST OBSERVATION TO ASSESS: {}'.format(list_[0]))
                print('BOOLEAN 1 - CAPITZALIDED: {}'.format(
                    StrHandler().is_capitalized(StrHandler().remove_diacritics(
                        list_[0])) == True
                ))
                print('BOOLEAN 2 - LOWER: {}'.format((StrHandler().remove_diacritics(
                    list_[0]).islower() == True)))
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

    def get_list_until_invalid_occurrences(self, list_, list_invalid_values):
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS
        """
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
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS
        """
        try:
            return next(iter([el for el in list_ if str(el).isnumeric()]))
        except StopIteration:
            return False

    def get_lower_upper_bound(self, sorted_list, value_to_put_in_between):
        """
        REFERENCES: https://stackoverflow.com/questions/55895500/need-to-check-if-value-is-between-two-numbers-in-a-list
        DOCSTRING: LOWER AND UPPER BOUND OF DATA THAT A VALUE IS IN BETWEEN; IT CONSIDERS A
            LIST IN ASCENDING ORDER
        INPUTS: SORTED LIST AND VALUE TO BE IN BETWEEN OF DATA WITHIN THE LIST
        OUTPUTS JSON WITH LOWER AND UPPER BOUND
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
            dict_message = {
                'lower_bound': sorted_list[list_idx_lower_upper_bound[0]],
                'upper_bound': sorted_list[list_idx_lower_upper_bound[1]]
            }
            return JsonFiles().send_json(dict_message)
        else:
            raise Exception('{} value is outside the bounds of {}'.format(
                value_to_put_in_between, sorted_list))

    def get_lower_mid_upper_bound(self, sorted_list, value_to_put_in_between):
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

    def closest_bound(self, sorted_list, value_to_put_in_between):
        """
        DOCSTRING: CLOSEST BOUND TO A VALUE IN A LIST
        INPUTS: SORTED LIST, VALUE TO PUT IN BETWEEN
        OUTPUTS: VALUE
        """
        return sorted_list[min(range(len(sorted_list)), key=lambda i:
                               abs(sorted_list[i] - value_to_put_in_between))]
    
    def closest_number_within_list(list_, number_):
        """
        DOCSTRING: CLOSEST NUMBER TO NUMBER_ WITHIN A LIST
        INPUTS: LIST OF NUMBERS (NOT NECESSARILY SORTED) AND NUMBER K
        OUTPUTS: FLOAT/INTEGER
        """
        return list_[min(range(len(list_)), key=lambda i: abs(list_[i]-number_))]

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
            list_extended_lists = HandlingLists().remove_duplicates(list_extended_lists)
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
            list_to_chunk = HandlingLists().remove_duplicates(list_to_chunk)
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
        list_chunked = HandlingLists().remove_duplicates(list_chunked)
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

    def pairwise(self, iterable):
        """
        REFERENCES: https://docs.python.org/3/library/itertools.html#itertools.pairwise
        DOCSTRING: RETURN SUCCESSIVE OVERLAPPING PAIRS TAKEN FROM THE INPUT ITERABLE
        INPUTS: ITERABLE (LIST)
        OUTPUTS: LIST OF TUPLES
        """
        a, b = tee(iterable)
        next(b, None)
        return list(zip(a, b))

    def discard_from_list(self, list_, list_items_remove):
        """
        DOCSTRING: DISCARD-LIKE, FROM SETS, TO LISTS
        INPUTS: LIST AND LIST OF ELEMENTS TO REMOVE
        OUTPUTS: -
        """
        for item in list_items_remove:
            if item in list_:
                list_.remove(item)
        return list_

    def absolute_frequency(self, list_):
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        return Counter(list_)

    def flatten_list(self, list_):
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        return [x for xs in list_ for x in xs]

class PriorityQueue:
    """
    REFERENCES: PYTHON COOKBOOK - DAVID BEASZLEY, BRIAN K. JONES
    DOCSTRING: CREATE A PRIORITY QUEUE LIST
    INPUTS: -
    OUTPUTS: -
    """

    def __init__(self):
        self._queue = []
        self._index = 0

    def push(self, item, priority):
        """
        DOCSTRING: PUSH ITEMS TO LIST
        INPUTS: ITEM, PRIORITY
        OUTPUTS: -
        """
        heappush(self._queue, (-priority, self._index, item))
        self._index += 1

    def pop(self):
        """
        DOCSTRING: REMOVE LAST ITEM FROM LIST
        INPUTS: -
        OUTPUTS: OBJECT
        """
        return heappop(self._queue)[-1]

# sorted_list = [10, 30, 52, 73, 90, 113, 133, 154, 175, 197, 219, 240, 303, 365, 427, 492, 554, 617, 678,
#                742, 803, 864, 927, 993, 1057, 1118, 1179, 1245, 1310, 1432, 1560, 1811, 2060, 2310, 2563, 3066, 3566]
# value_needle = 2564
# print(HandlingLists().get_lower_mid_upper_bound(sorted_list, value_needle))
# print(sorted_list)
# print(value_needle)
# # output
# {'lower_bound': 2563, 'middle_bound': 3066, 'upper_bound': 3566, 'end_of_list': True}

# sorted_list = [10, 30, 52, 73, 90, 113, 133, 154, 175, 197, 219, 240, 303, 365, 427, 492, 554, 617, 678,
#                742, 803, 864, 927, 993, 1057, 1118, 1179, 1245, 1310, 1432, 1560, 1811, 2060, 2310, 2563, 3066, 3566]
# value_needle = 3100
# print(HandlingLists().get_lower_upper_bound(sorted_list, value_needle))
# print(sorted_list)
# print(value_needle)
# # output
# {'lower_bound': 3066, 'upper_bound': 3566}
# list_ = ['Bacterias', 'mesofilas', 'aerobias', 'e', 'anaerobias',
#          'facultativas', '–', 'ISO', '4833-2.', '2013', 'FDA.', 'BAM.', 'Ch']
# print(HandlingLists().get_first_occurrence_within_list(
#     list_, bl_last_uppercase_before_capitalized=True))
# list_ = ['SWAB', 'DE', 'CARCAÇAS', 'S.', 'aureus', '–',
#          'Determinação', 'quantitativa', 'pela', 'técnica', 'de']
# print(HandlingLists().get_first_occurrence_within_list(
#     list_, bl_last_uppercase_before_capitalized=True))
