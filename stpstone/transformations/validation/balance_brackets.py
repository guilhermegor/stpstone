from stpstone.dsa.stacks.simple_stack import Stack


class BalanceBrackets:

    def is_balanced(self, expression: str) -> bool:
        bracket_map = {')': '(', '}': '{', ']': '['}
        open_brackets = set(bracket_map.values())
        cls_stack = Stack()
        for char in expression:
            if char in open_brackets:
                cls_stack.push(char)
            elif char in bracket_map:
                if cls_stack.is_empty or cls_stack.pop() != bracket_map[char]:
                    return False
        return cls_stack.is_empty
