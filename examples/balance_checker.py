from stpstone.transformations.validation.balance_brackets import BalanceBrackets


balance_brackets = BalanceBrackets()

# 1. syntax validation in code editors or IDEs (with function input)
def validate_function_syntax(func_str: str) -> bool:
    return balance_brackets.is_balanced(func_str)

function_code = """
def foo():
    if (x > 0):
        return [1, 2, 3]
"""

print("1.1. Function Syntax Validation:", validate_function_syntax(function_code))

unbalanced_function_code = """
def foo():
    if (x > 0:
        return [1, 2, 3]
"""

print("1.2. Unbalanced Function Syntax Validation:", validate_function_syntax(unbalanced_function_code))

# 2. mathematical expression Evaluation
expression = "((a + b) * (c - d))"
print("2. Mathematical Expression:", balance_brackets.is_balanced(expression))

# 3. configuration file validation
json_data = '{"key": {"nested_key": [1, 2, 3]}}'
print("3. JSON Validation:", balance_brackets.is_balanced(json_data))

# 4. web form input validation
user_input = "({[Hello, World!]})"
print("4. Web Form Input:", balance_brackets.is_balanced(user_input))

# 5. parsing and processing markup languages
html_snippet = "<div><p>Hello</p></div>"
print("5. HTML Validation:", balance_brackets.is_balanced(html_snippet))

# 6. financial formula validation
formula = "=SUM((A1 + B1) * (C1 - D1))"
print("6. Financial Formula:", balance_brackets.is_balanced(formula))

# 7. data cleaning and preprocessing
dirty_text = "This is a {badly [formatted] string."
print("7. Data Cleaning:", balance_brackets.is_balanced(dirty_text))

# 8. compiler or interpreter development
source_code = "if (x > 0) { print('Hello'); }"
print("8. Source Code Validation:", balance_brackets.is_balanced(source_code))

# 9. network protocol validation
protocol_message = "{ 'type': 'request', 'data': [1, 2, 3] }"
print("9. Network Protocol:", balance_brackets.is_balanced(protocol_message))

# 10. database query validation
sql_query = "SELECT * FROM users WHERE (age > 18 AND (status = 'active'))"
print("10. SQL Query:", balance_brackets.is_balanced(sql_query))

# 11. template engine validation
template = "{% if user.is_authenticated %}<p>Welcome, {{ user.name }}!</p>{% endif %}"
print("11. Template Validation:", balance_brackets.is_balanced(template))

# 12. command-line argument validation
command = "run --input {file.txt} --output [result.json]"
print("12. Command-Line Argument:", balance_brackets.is_balanced(command))

# 13. log file analysis
log_entry = "{ 'timestamp': '2023-10-01', 'message': 'Error occurred' }"
print("13. Log Entry:", balance_brackets.is_balanced(log_entry))
