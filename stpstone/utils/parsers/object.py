### HANDLING OBJECTS OF A MYRIAD OF DATATYPES ###

import ast
from stpstone.utils.parsers.str import StrHandler


class HandlingObjects:

    def literal_eval_data(self, data_object, str_left_bound=None, str_right_bound=None):
        """
        DOCSTRING: LITERAL EVAL AN OBJECT TO ITS INHERENT TYPE, FOR INSTANCE A STRING IN LIST
            OR DICT FORMAT
        INPUTS: DATA OBJECT, STRING LEFT BOUND, STRING RIGHT BOUND
        OUTPUTS:
        """
        if any([x is None for x in [str_left_bound, str_right_bound]]):
            return ast.literal_eval(data_object)
        else:
            return ast.literal_eval(StrHandler().get_between(
                str(data_object), str_left_bound, str_right_bound))
