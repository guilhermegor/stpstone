#!/bin/bash

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
MAGENTA='\033[0;35m'
NC='\033[0m'

print_status() {
    local status="$1"
    local message="$2"
    case "$status" in
        "success") echo -e "${GREEN}[✓]${NC} ${message}" ;;
        "error") echo -e "${RED}[✗]${NC} ${message}" >&2 ;;
        "warning") echo -e "${YELLOW}[!]${NC} ${message}" ;;
        "info") echo -e "${BLUE}[i]${NC} ${message}" ;;
        "config") echo -e "${CYAN}[→]${NC} ${message}" ;;
        "debug") echo -e "${MAGENTA}[»]${NC} ${message}" ;;
        *) echo -e "[ ] ${message}" ;;
    esac
}

find_module_path() {
    local module="$1"
    local module_path=$(find stpstone -name "${module}.py" -not -path "*__pycache__*" | head -1)
    if [ -z "$module_path" ]; then
        print_status "error" "Module ${module}.py not found in stpstone/"
        return 1
    fi
    echo "$module_path"
}

find_test_path() {
    local module="$1"
    local test_path=$(find tests -name "test_${module}.py" -o -name "${module}_test.py" -not -path "*__pycache__*" | head -1)
    if [ -z "$test_path" ]; then
        print_status "error" "Test file for ${module} not found (looking for test_${module}.py or ${module}_test.py)"
        return 1
    fi
    echo "$test_path"
}

run_codespell() {
    local path="$1"
    local type="$2"
    print_status "info" "Running codespell on ${type}: ${path}"
    if ! poetry run codespell "$path"; then
        print_status "error" "codespell found spelling issues in ${type}"
        print_status "warning" "To interactively fix, run: poetry run codespell -w ${path}"
        return 1
    fi
    print_status "success" "codespell passed for ${type}"
}

check_type_consistency() {
    local path="$1"
    local type="$2"
    
    print_status "info" "Checking type hint/docstring consistency for ${type}: ${path}"
    
    # First check with pydocstyle (numpy convention)
    if ! poetry run pydocstyle --select=D412,D417,DAR "$path"; then
        print_status "error" "pydocstyle found docstring issues in ${type}"
        print_status "warning" "Some return/parameter types may not match type hints"
        return 1
    fi
    
    # Custom check using python ast parsing
    local temp_file=$(mktemp)
    cat << 'EOF' > "$temp_file"
import ast
import sys
from typing import Any

def compare_types(hint: Any, doc: str) -> bool:
    """Compare type hint with docstring type."""
    if hint is Any or doc.lower() == "any":
        return True
        
    hint_str = str(hint).replace("typing.", "").lower()
    doc = doc.lower().strip()
    
    # Handle common equivalences
    equivalences = {
        "list": "sequence",
        "dict": "mapping",
        "np.ndarray": "ndarray",
        "numpy.ndarray": "ndarray"
    }
    
    for k, v in equivalences.items():
        hint_str = hint_str.replace(k.lower(), v)
        doc = doc.replace(k.lower(), v)
    
    return hint_str == doc

def check_file(filepath: str) -> int:
    errors = 0
    with open(filepath, 'r') as f:
        tree = ast.parse(f.read())
    
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            if not node.returns and not any(isinstance(decorator, ast.Name) and 
               decorator.id == 'property' for decorator in node.decorator_list):
                continue
                
            docstring = ast.get_docstring(node)
            if not docstring:
                print(f"⚠️  Missing docstring in {node.name}()")
                continue
                
            # Check return type
            if node.returns:
                returns = ast.unparse(node.returns)
                if "Returns" in docstring or "returns" in docstring:
                    doc_lines = [l.strip() for l in docstring.split('\n')]
                    found_return = False
                    for i, line in enumerate(doc_lines):
                        if "return" in line.lower() and ":" in line:
                            doc_type = line.split(":")[1].strip()
                            if not compare_types(returns, doc_type):
                                print(f"❌ Return type mismatch in {node.name}():")
                                print(f"   Type hint: {returns}")
                                print(f"   Docstring: {doc_type}")
                                errors += 1
                            found_return = True
                            break
                    if not found_return:
                        print(f"⚠️  Return type documented but no type hint in {node.name}()")
            
            # Check parameters
            for arg in node.args.args:
                if arg.arg == 'self':
                    continue
                    
                if arg.annotation:
                    hint = ast.unparse(arg.annotation)
                    arg_doc_found = False
                    for line in docstring.split('\n'):
                        if arg.arg in line and ":" in line:
                            doc_type = line.split(":")[1].strip()
                            if not compare_types(hint, doc_type):
                                print(f"❌ Parameter type mismatch in {node.name}({arg.arg}):")
                                print(f"   Type hint: {hint}")
                                print(f"   Docstring: {doc_type}")
                                errors += 1
                            arg_doc_found = True
                            break
                    if not arg_doc_found:
                        print(f"⚠️  Missing docstring for parameter {arg.arg} in {node.name}()")
    
    return errors

if __name__ == "__main__":
    errors = check_file(sys.argv[1])
    sys.exit(1 if errors > 0 else 0)
EOF

    if ! python "$temp_file" "$path"; then
        print_status "error" "Type hint/docstring inconsistencies found in ${type}"
        rm "$temp_file"
        return 1
    fi
    
    rm "$temp_file"
    print_status "success" "Type hints and docstrings are consistent for ${type}"
    return 0
}

run_ruff() {
    local path="$1"
    local type="$2"
    print_status "info" "Running Ruff on ${type}: ${path}"
    if ! poetry run ruff check "$path"; then
        print_status "error" "Ruff checks failed for ${type}"
        return 1
    fi
    print_status "success" "Ruff checks passed for ${type}"
}

run_pytest() {
    local test_path="$1"
    print_status "info" "Running pytest: ${test_path}"
    if ! poetry run pytest "$test_path" -v; then
        print_status "error" "pytest failed"
        return 1
    fi
    print_status "success" "pytest passed"
}

main() {
    if [ -z "$1" ]; then
        print_status "error" "Usage: $0 <module_name>"
        exit 1
    fi

    local module="$1"
    print_status "info" "=== Testing module: ${module} ==="

    local module_path
    module_path=$(find_module_path "$module") || exit 1
    local test_path
    test_path=$(find_test_path "$module") || exit 1

    run_codespell "$module_path" "module" || return 1
    run_codespell "$test_path" "tests" || return 1
    check_type_consistency "$module_path" "module" || return 1
    check_type_consistency "$test_path" "module" || return 1
    run_ruff "$module_path" "module" || return 1
    run_ruff "$test_path" "tests" || return 1
    run_pytest "$test_path" || return 1

    print_status "success" "=== All checks passed for ${module} ==="
}

main "$@"