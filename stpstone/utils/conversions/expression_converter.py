from typing import Literal
from stpstone.dsa.stacks.simple_stack import Stack
from stpstone.transformations.validation.metaclass_type_checker import TypeChecker


class ExpressionConverter(metaclass=TypeChecker):

    def __init__(
        self,
        str_expr: str,
        str_from_type: Literal["infix", "postfix", "prefix"],
        str_to_type: Literal["infix", "postfix", "prefix"]
    ) -> None:
        self.str_expr = str_expr
        self.str_from_type = str_from_type
        self.str_to_type = str_to_type
        self.prec = {}
        self.prec["*"] = 3
        self.prec["/"] = 3
        self.prec["+"] = 2
        self.prec["-"] = 2
        self.prec["("] = 1
        self.postfix_list = []
        self.token_list = str_expr.upper().split(" ")
        self.str_operands = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
        self.str_operators = "+-*/()"

    @property
    def _infix_to_postfix(self) -> str:
        cls_stack = Stack()
        for token in self.token_list:
            if token in self.str_operands:
                self.postfix_list.append(token)
            elif token == "(":
                cls_stack.push(token)
            elif token == ")":
                top_token = cls_stack.pop()
                while top_token != "(":
                    self.postfix_list.append(top_token)
                    top_token = cls_stack.pop()
            else:
                while (not cls_stack.is_empty) and (self.prec[cls_stack.peek] >= self.prec[token]):
                    self.postfix_list.append(cls_stack.pop())
                cls_stack.push(token)
        while not cls_stack.is_empty:
            self.postfix_list.append(cls_stack.pop())
        return " ".join(self.postfix_list)

    @property
    def _infix_to_prefix(self) -> str:
        postfix_expr = self._infix_to_postfix
        cls_stack = Stack()
        for token in postfix_expr.split(" "):
            if token in self.str_operands:
                cls_stack.push(token)
            else:
                right = cls_stack.pop()
                left = cls_stack.pop()
                expr = f"{token} {left} {right}"
                cls_stack.push(expr)
        return cls_stack.pop()

    @property
    def _postfix_to_infix(self) -> str:
        cls_stack = Stack()
        for token in self.token_list:
            if token in self.str_operands:
                cls_stack.push(token)
            else:
                right = cls_stack.pop()
                left = cls_stack.pop()
                expr = f"({left} {token} {right})"
                cls_stack.push(expr)
        return cls_stack.pop()

    @property
    def _postfix_to_prefix(self) -> str:
        cls_stack = Stack()
        for token in self.token_list:
            if token in self.str_operands:
                cls_stack.push(token)
            else:
                right = cls_stack.pop()
                left = cls_stack.pop()
                expr = f"{token} {left} {right}"
                cls_stack.push(expr)
        return cls_stack.pop()

    @property
    def _prefix_to_infix(self) -> str:
        cls_stack = Stack()
        for token in reversed(self.token_list):
            if token in self.str_operands:
                cls_stack.push(token)
            else:
                left = cls_stack.pop()
                right = cls_stack.pop()
                expr = f"({left} {token} {right})"
                cls_stack.push(expr)
        return cls_stack.pop()

    @property
    def _prefix_to_postfix(self) -> str:
        cls_stack = Stack()
        for token in reversed(self.token_list):
            if token in self.str_operands:
                cls_stack.push(token)
            else:
                left = cls_stack.pop()
                right = cls_stack.pop()
                expr = f"{left} {right} {token}"
                cls_stack.push(expr)
        return cls_stack.pop()

    @property
    def convert(self) -> str:
        if self.str_from_type == "infix" and self.str_to_type == "postfix":
            return self._infix_to_postfix
        elif self.str_from_type == "infix" and self.str_to_type == "prefix":
            return self._infix_to_prefix
        elif self.str_from_type == "postfix" and self.str_to_type == "infix":
            return self._postfix_to_infix
        elif self.str_from_type == "postfix" and self.str_to_type == "prefix":
            return self._postfix_to_prefix
        elif self.str_from_type == "prefix" and self.str_to_type == "infix":
            return self._prefix_to_infix
        elif self.str_from_type == "prefix" and self.str_to_type == "postfix":
            return self._prefix_to_postfix
        else:
            raise ValueError("Invalid conversion type")
