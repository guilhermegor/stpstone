from stpstone.utils.parsers.numbers import NumHandler

num_converter = NumHandler().transform_to_float

test_cases = [
    "3,132.45%",          # American format with % -> 31.3245
    "3.134,4343424%",     # European format with % -> 31.344343424
    "314.124.434,3333555", # European big number -> 314124434.3333555
    "1.234,56",           # European format -> 1234.56
    "1,234.56",           # American format -> 1234.56
    "1.234.567,89",       # European big number -> 1234567.89
    "1,234,567.89",       # American big number -> 1234567.89
    "4.56 bp",            # Basis points -> 0.000456
    "(-)912.412.911,231", # European negative -> -912412911.231
    "Text Tried",         # Should remain unchanged
    True,                  # Should remain True
    "55,987,544",     # European big number -> 55987544
    "846.874.688",      # European big number -> 846874688
    "-0,10%",           # European negative -> -0.1
    "0,10%",            # European positive -> 0.1
]

print("Testing with precision=2")
for case in test_cases:
    result = num_converter(case, int_precision=2)
    print(f"{str(case):<25} -> {result}")

print("\nTesting with precision=6")
for case in test_cases:
    result = num_converter(case, int_precision=6)
    print(f"{str(case):<25} -> {result}")