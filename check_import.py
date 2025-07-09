#!/usr/bin/env python3
import importlib
import json
import sys


def main():
    """Main function."""
    if len(sys.argv) < 2:
        print("Usage: check_import.py <json_dict_info>")
        sys.exit(1)
        
    dict_info = json.loads(sys.argv[1])
    print(f'[Pre-Run Check] {dict_info["name"]} {dict_info["version"]}')
    importlib.import_module(dict_info["name"].replace('-', '_'))
    print('Import successful')

if __name__ == "__main__":
    main()