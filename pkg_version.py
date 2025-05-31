#!/usr/bin/env python3
import sys
import re
import json
import toml
from pathlib import Path


def get_package_info():
    """Detect package name and version from pyproject.toml or setup.py"""
    try:
        # Try pyproject.toml first (Poetry style)
        with open('pyproject.toml') as f:
            data = toml.load(f)
            if 'tool' in data and 'poetry' in data['tool']:
                return {
                    'name': data['tool']['poetry']['name'],
                    'version': data['tool']['poetry']['version']
                }
        
        # Try setup.py as fallback
        with open('setup.py') as f:
            content = f.read()
            name_match = re.search(r'name=[\'\"](.+?)[\'\"]', content)
            version_match = re.search(r'version=[\'\"](.+?)[\'\"]', content)
            
            if name_match and version_match:
                return {
                    'name': name_match.group(1),
                    'version': version_match.group(1)
                }
                
    except Exception as e:
        sys.stderr.write(f"Error detecting package info: {str(e)}\n")
    
    sys.exit("Could not detect package name and version")

if __name__ == "__main__":
    info = get_package_info()
    print(json.dumps(info))