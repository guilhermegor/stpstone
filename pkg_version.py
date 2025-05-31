import sys
import re

try:
    import toml
    with open('pyproject.toml') as f:
        print(toml.load(f)['tool']['poetry']['version'])
except Exception:
    with open('setup.py') as f:
        match = re.search(r'version=[\'\"](.+?)[\'\"]', f.read())
        print(match.group(1) if match else sys.exit('Could not detect version'))