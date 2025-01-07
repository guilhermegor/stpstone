### VALIDATE PROPERTIES AND METHODS FROM A CLASS ###

from pydantic import validate_arguments


class ValidateAllMethodsMeta(type):
    '''
    DOCSTRING: VALIDATE ALL INPUT AND OUTPUT TYPES OF A CLASS
    INPUTS: NAME, BASES, DCT
    OUTPUTS: VALIDATED CLASS
    '''
    def __new__(cls, name, bases, dct):
        for attr_name, attr_value in dct.items():
            if callable(attr_value) and not attr_name.startswith("__"):
                dct[attr_name] = validate_arguments(attr_value)
        return super().__new__(cls, name, bases, dct)


if __name__ == '__main__':

    class MyClass(metaclass=ValidateAllMethodsMeta):

        def method_one(self, x: int, y: int) -> int:
            return x + y

        def method_two(self, z: str) -> str:
            return z.upper()


    # usage
    obj = MyClass()
    print(obj.method_one(3, 4))  # Outputs: 7
    try:
        print(obj.method_one("a", "b"))  # Raises ValidationError
    except Exception as e:
        print("Validation Error:", e)