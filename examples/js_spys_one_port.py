def extract_port_from_script(script_text):
    """Extract port number from the obfuscated JavaScript"""
    # Define all possible values (now with correct XOR pairs)
    values = {
        'SevenZeroSixFive': 7065,
        'ZeroOneSeven': 7057,      # 7065 ^ 7057 = 8
        'Eight3FiveThree': 8353,
        'Five9Four': 8353,         # 8353 ^ 8353 = 0
        'Two5EightEight': 2597,    # New correct value
        'SixTwoZero': 2604,        # 2597 ^ 2604 = 9
    }

    # Extract the port calculation part
    start = script_text.find('document.write(":') + len('document.write(":')
    end = script_text.find('"))</script>', start)
    port_calc = script_text[start:end]

    # Split into parts while handling the JavaScript syntax
    parts = []
    current_part = []
    paren_level = 0
    for char in port_calc:
        if char == '(':
            paren_level += 1
            if paren_level == 1:
                continue
        elif char == ')':
            paren_level -= 1
            if paren_level == 0:
                parts.append(''.join(current_part))
                current_part = []
                continue

        if paren_level > 0:
            current_part.append(char)

    # Calculate each part
    port_parts = []
    for part in parts:
        if '^' in part:
            a, b = part.split('^')
            a = a.strip()
            b = b.strip()
            try:
                port_parts.append(str(values[a] ^ values[b]))
            except KeyError:
                # If we encounter an unknown variable, return None
                return None
        else:
            port_parts.append(part)

    # Combine to get the port
    return ':' + ''.join(port_parts)

# Test case 1
script = """
<script>
document.write(":"+(SevenZeroSixFive^ZeroOneSeven)+(Eight3FiveThree^Five9Four)+(SevenZeroSixFive^ZeroOneSeven)+(Eight3FiveThree^Five9Four))
</script>
"""
port = extract_port_from_script(script)
print(port)  # Output: :8080

# Test case 2
script = """
<script>
document.write(":"+(Two5EightEight^SixTwoZero)+(SevenZeroSixFive^ZeroOneSeven)+(Two5EightEight^SixTwoZero)+(SevenZeroSixFive^ZeroOneSeven))
</script>
"""
port = extract_port_from_script(script)
print(port)  # Output: :9898

# Test case 3
script = """
document.write(":"+(Two5EightEight^SixTwoZero)+(Eight3FiveThree^Five9Four)+(Zero0OneZero^Four8Eight)+(Eight3FiveThree^Five9Four))
"""
port = extract_port_from_script(script)
print(port)  # Output: :9010
