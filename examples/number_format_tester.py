from stpstone.utils.parsers.numbers import NumHandler

num_converter = NumHandler().transform_to_float

test_cases = [
    "3,132.45%",          # american format with % -> 31.3245
    "3.134,4343424%",     # european format with % -> 31.344343424
    "314.124.434,3333555", # european big number -> 314124434.3333555
    "1.234,56",           # european format -> 1234.56
    "1,234.56",           # american format -> 1234.56
    "1.234.567,89",       # european big number -> 1234567.89
    "1,234,567.89",       # american big number -> 1234567.89
    "4.56 bp",            # basis points -> 0.000456
    "(-)912.412.911,231", # european negative -> -912412911.231
    "text tried",         # should remain unchanged
    "Example TEXT",       # should remain unchanged
    "9888 Example",       # should remain unchanged
    True,                  # should remain true
    "55,987,544",     # european big number -> 55987544
    "846.874.688",      # european big number -> 846874688
    "-0,10%",           # european negative -> -0.1
    "0,10%",            # european positive -> 0.1
    "(912.412.911,231)", # should return a negative number
    "(81,234,567.1324432)",      # should return a negative number
    "058,234,567.1324432",      # should return a positive number
    "-0845547,789789",           # should return a negative number
    "085.944,789789",            # should return a positive number
    "EXAMPLE (912.412.911,231)", # should remain unchanged
    "EXAMPLE (81,234,567.1324432)", # should remain unchanged
]

print("Testing with precision=2")
for case in test_cases:
    result = num_converter(case, int_precision=2)
    print(f"{str(case):<25} -> {result}")

print("\nTesting with precision=6")
for case in test_cases:
    result = num_converter(case, int_precision=6)
    print(f"{str(case):<25} -> {result}")