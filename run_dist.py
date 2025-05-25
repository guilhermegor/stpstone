from stpstone.analytics.pricing.derivatives.european_options import EuropeanOptions
from stpstone.utils.cals.handling_dates import DatesBR
from stpstone.utils.conversions.base_converter import BaseConverter
from stpstone.utils.conversions.expression_converter import ExpressionConverter
from stpstone.utils.geography.br import BrazilGeo
from stpstone.utils.parsers.numbers import NumHandler
from stpstone.utils.parsers.lists import ListHandler


print("*** Dates BR Tester ***")
print(f"Current Date: {DatesBR().curr_date}")
print(f"3 working days before current date: {DatesBR().sub_working_days(DatesBR().curr_date, 3)}")
print(f"3 working days after current date: {DatesBR().add_working_days(DatesBR().curr_date, 3)}")

print("\n*** Unix Timestamp Tester ***")
print(f"Current Unix Timestamp: {DatesBR().datetime_to_unix_timestamp(DatesBR().curr_date)}")

print("\n*** European Options Tester ***")
print(f"Black Scholes Call Option: {EuropeanOptions().general_opt_price(103.0, 100.0, 0.025, 0.25, 0.2, 0.0, 0.0, 'call')}")
print(f"Black Scholes Put Option: {EuropeanOptions().general_opt_price(103.0, 100.0, 0.025, 0.25, 0.2, 0.0, 0.0, 'put')}")

print("\n*** Base Converter Tester ***")
print(f"Decimal to Binary: {BaseConverter("1027", 10, 2).convert}")
print(f"Decimal to Hexadecimal: {BaseConverter("1027", 10, 16).convert}")
print(f"Decimal to Octal: {BaseConverter("1027", 10, 8).convert}")

print("\n***Expression Converter Tester ***")

# example 1: infix to postfix conversion
print(f"Infix to Postfix (A + B * C): {ExpressionConverter('A + B * C', 'infix', 'postfix').convert}")
print(f"Infix to Postfix (( A + B ) * C): {ExpressionConverter('( A + B ) * C', 'infix', 'postfix').convert}")
print(f"Infix to Postfix (A + B * C - D / E): {ExpressionConverter('A + B * C - D / E', 'infix', 'postfix').convert}")

print("\n*** Expression Converter Postfix to Infix ***")

# example 2: postfix to infix conversion
print(f"Postfix to Infix (A B +): {ExpressionConverter('A B +', 'postfix', 'infix').convert}")
print(f"Postfix to Infix (A B + C *): {ExpressionConverter('A B + C *', 'postfix', 'infix').convert}")
print(f"Postfix to Infix (A B C * + D E / -): {ExpressionConverter('A B C * + D E / -', 'postfix', 'infix').convert}")

print("\n*** Expression Converter Prefix Conversions ***")

# example 3: prefix to postfix and prefix to infix conversions
print(f"Prefix to Postfix (+ A B): {ExpressionConverter('+ A B', 'prefix', 'postfix').convert}")
print(f"Prefix to Infix (+ A B): {ExpressionConverter('+ A B', 'prefix', 'infix').convert}")
print(f"Prefix to Postfix (* + A B C): {ExpressionConverter('* + A B C', 'prefix', 'postfix').convert}")
print(f"Prefix to Infix (* + A B C): {ExpressionConverter('* + A B C', 'prefix', 'infix').convert}")

print("\n*** Expression Converter Advanced Examples ***")

# additional complex examples
print(f"Infix to Prefix (A + B * C): {ExpressionConverter('A + B * C', 'infix', 'prefix').convert}")
print(f"Infix to Prefix (( A + B ) * ( C - D )): {ExpressionConverter('( A + B ) * ( C - D )', 'infix', 'prefix').convert}")
print(f"Postfix to Prefix (A B + C D - *): {ExpressionConverter('A B + C D - *', 'postfix', 'prefix').convert}")

print("\n*** Expression Converter with Numbers ***")

# Examples with numeric operands
print(f"Infix to Postfix (1 + 2 * 3): {ExpressionConverter('1 + 2 * 3', 'infix', 'postfix').convert}")
print(f"Postfix to Infix (1 2 + 3 *): {ExpressionConverter('1 2 + 3 *', 'postfix', 'infix').convert}")
print(f"Prefix to Postfix (+ 1 * 2 3): {ExpressionConverter('+ 1 * 2 3', 'prefix', 'postfix').convert}")

print("\n*** Brazil Geo Tester ***")
print(f"Capital Name: {BrazilGeo().states(True)}")

print("\n*** Num Handler Tester ***")
print(f"Num Handler: {NumHandler().transform_to_float('1,000.00')}")

print("\n*** List Handler Basic Operations ***")

# Example 1: Remove duplicates
print(f"Remove Duplicates: {ListHandler().remove_duplicates([1, 2, 2, 3, 3, 4, 5, 5])}")
print(f"Remove Duplicates (strings): {ListHandler().remove_duplicates(['apple', 'banana', 'apple', 'cherry', 'banana'])}")

# Example 2: First numeric value
print(f"First Numeric: {ListHandler().first_numeric(['abc', 'def', '123', '456'])}")
print(f"First Numeric (no numbers): {ListHandler().first_numeric(['abc', 'def', 'ghi'])}")

# Example 3: Extend multiple lists
print(f"Extend Lists: {ListHandler().extend_lists([1, 2, 3], [4, 5, 6], [7, 8, 9])}")
print(f"Extend Lists (with duplicates): {ListHandler().extend_lists([1, 2, 3], [3, 4, 5], [5, 6, 7], bl_remove_duplicates=False)}")

print("\n*** List Handler Mathematical Operations ***")
# Example 4: Find closest number
print(f"Closest Number to 7.5: {ListHandler().closest_number_within_list([1, 5, 10, 15, 20], 7.5)}")
print(f"Closest Bound to 12: {ListHandler().closest_bound([1, 5, 10, 15, 20], 12)}")

# Example 5: Lower and upper bounds
print(f"Lower Upper Bound (12 in [1,5,10,15,20]): {ListHandler().get_lower_upper_bound([1, 5, 10, 15, 20], 12)}")
print(f"Lower Upper Bound (10 in [1,5,10,15,20]): {ListHandler().get_lower_upper_bound([1, 5, 10, 15, 20], 10)}")

# Example 6: Nth smallest numbers
print(f"3 Smallest Numbers: {ListHandler().nth_smallest_numbers([15, 3, 8, 1, 12, 7, 20], 3)}")

print("\n*** List Handler String Operations ***")
# Example 7: Sort alphanumeric
print(f"Sort Alphanumeric: {ListHandler().sort_alphanumeric(['item10', 'item2', 'item1', 'item20'])}")

# Example 8: First occurrence operations
test_list = ['apple', 'BANANA', 'cherry', 'DATE']
print(f"First Uppercase: {ListHandler().get_first_occurrence_within_list(test_list, bl_uppercase=True)}")
# Using exact match that exists in the list to avoid StopIteration error
print(f"First Occurrence Like 'banana': {ListHandler().first_occurrence_like(['apple', 'banana', 'cherry'], 'banana')}")
print(f"First Occurrence with obj_occurrence: {ListHandler().get_first_occurrence_within_list(['apple', 'banana', 'cherry'], obj_occurrence='banana')}")

print("\n*** List Handler Advanced Operations ***")
# Example 9: Absolute frequency
print(f"Absolute Frequency: {ListHandler().absolute_frequency(['a', 'b', 'a', 'c', 'b', 'a'])}")

# Example 10: Flatten nested list
print(f"Flatten List: {ListHandler().flatten_list([[1, 2], [3, 4], [5, 6]])}")

# Example 11: Remove consecutive duplicates
print(f"Remove Consecutive Duplicates: {ListHandler().remove_consecutive_duplicates([1, 1, 2, 2, 2, 3, 1, 1])}")

# Example 12: Pairwise combinations
print(f"Pairwise: {ListHandler().pairwise([1, 2, 3, 4, 5])}")

print("\n*** List Handler Chunk and Cartesian Operations ***")
# Example 13: Chunk list
print(f"Chunk List (size 3): {ListHandler().chunk_list(['a', 'b', 'c', 'd', 'e', 'f', 'g'], str_character_divides_clients=None, int_chunk=3)}")
print(f"Chunk List (joined): {ListHandler().chunk_list(['apple', 'banana', 'cherry', 'date', 'elderberry'], int_chunk=2)}")

# Example 14: Cartesian product
print(f"Cartesian Product: {ListHandler().cartesian_product([['A', 'B'], [1, 2]])}")
print(f"Cartesian Product (limited): {ListHandler().cartesian_product([['A', 'B'], [1, 2], ['X', 'Y']], int_break_n_n=2)}")

print("\n*** List Handler Utility Operations ***")
# Example 15: Discard items from list
print(f"Discard Items: {ListHandler().discard_from_list([1, 2, 3, 4, 5], [2, 4])}")

# Example 16: Replace occurrences
test_replace_list = ['apple', 'banana', 'apple', 'cherry', 'apple']
print(f"Replace First 'apple' with 'orange': {ListHandler().replace_first_occurrence(test_replace_list.copy(), 'apple', 'orange')}")
print(f"Replace Last 'apple' with 'grape': {ListHandler().replace_last_occurrence(test_replace_list.copy(), 'apple', 'grape')}")

# Example 17: Get list until invalid occurrences
print(f"Get List Until Invalid: {ListHandler().get_list_until_invalid_occurrences(['valid1', 'valid2', 'STOP', 'invalid1'], ['STOP', 'END'])}")

print("\n*** List Handler Error Handling Examples ***")
# Example 18: Handling potential errors with try-catch
print("Testing methods that might raise exceptions:")
try:
    result = ListHandler().first_occurrence_like(['apple', 'banana', 'cherry'], 'xyz')
    print(f"First Occurrence Like 'xyz' (not found): {result}")
except StopIteration:
    print("First Occurrence Like 'xyz' (not found): No match found")

try:
    result = ListHandler().get_lower_upper_bound([1, 5, 10], 25)
    print(f"Lower Upper Bound (25 outside range): {result}")
except Exception as e:
    print(f"Lower Upper Bound (25 outside range): Error - Value outside bounds")