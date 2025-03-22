from typing import Literal
from stpstone.dsa.stacks.simple_stack import Stack

class ExpressionConverter:

    def __init__(self):
        self.prec = {}
        self.prec["*"] = 3
        self.prec["/"] = 3
        self.prec["+"] = 2
        self.prec["-"] = 2
        self.prec["("] = 1

    def _infix_to_postfix(self, infix_expr: str) -> str:
        op_stack = Stack()
        postfix_list = []
        token_list = infix_expr.split()
        for token in token_list:
            if token.isalnum():
                postfix_list.append(token)
            elif token == "(":
                op_stack.push(token)
            elif token == ")":
                top_token = op_stack.pop()
                while top_token != "(":
                    postfix_list.append(top_token)
                    top_token = op_stack.pop()
            else:
                while (not op_stack.is_empty()) and (self.prec[op_stack.peek()] >= self.prec[token]):
                    postfix_list.append(op_stack.pop())
                op_stack.push(token)
        while not op_stack.is_empty():
            postfix_list.append(op_stack.pop())
        return " ".join(postfix_list)

    def _infix_to_prefix(self, infix_expr: str) -> str:
        infix_expr = infix_expr[::-1]
        infix_expr = self._reverse_parentheses(infix_expr)
        op_stack = Stack()
        prefix_list = []
        token_list = infix_expr.split()
        for token in token_list:
            if token.isalnum():
                prefix_list.append(token)
            elif token == ")":
                op_stack.push(token)
            elif token == "(":
                top_token = op_stack.pop()
                while top_token != ")":
                    prefix_list.append(top_token)
                    top_token = op_stack.pop()
            else:
                while (not op_stack.is_empty()) and (self.prec[op_stack.peek()] > self.prec[token]):
                    prefix_list.append(op_stack.pop())
                op_stack.push(token)
        while not op_stack.is_empty():
            prefix_list.append(op_stack.pop())
        return " ".join(prefix_list[::-1])

    def _postfix_to_infix(self, postfix_expr: str) -> str:
        operand_stack = Stack()
        token_list = postfix_expr.split()
        for token in token_list:
            if token.isalnum():
                operand_stack.push(token)
            else:
                operand2 = operand_stack.pop()
                operand1 = operand_stack.pop()
                infix_expr = f"({operand1} {token} {operand2})"
                operand_stack.push(infix_expr)

        return operand_stack.pop()

    def _reverse_parentheses(self, str_expr: str) -> str:
        str_expr = str_expr.replace("(", "temp").replace(")", "(").replace("temp", ")")
        return str_expr

    def convert(
        self,
        str_expr: str,
        from_type: Literal["infix", "postfix", "prefix"],
        to_type: Literal["infix", "postfix", "prefix"]
    ) -> str:
        """
        Converts an expression from one type to another (infix, postfix, or prefix).

        Args:
            str_expr (str): The expression to be converted.
            from_type (Literal): The current type of the expression ("infix", "postfix", "prefix").
            to_type (Literal): The desired type of the expression ("infix", "postfix", "prefix").

        Returns:
            str: The converted expression.
        """
        if from_type == "infix" and to_type == "postfix":
            return self._infix_to_postfix(str_expr)
        elif from_type == "infix" and to_type == "prefix":
            return self._infix_to_prefix(str_expr)
        elif from_type == "postfix" and to_type == "infix":
            return self._postfix_to_infix(str_expr)
        else:
            raise ValueError("Invalid conversion type specified!")
