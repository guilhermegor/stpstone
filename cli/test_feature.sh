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
    
    # first check with pydocstyle (numpy convention)
    if ! poetry run pydocstyle "$path"; then
        print_status "error" "pydocstyle found docstring issues in ${type}"
        print_status "warning" "Some return/parameter types may not match type hints"
        return 1
    fi
    
    # custom check using python ast parsing
    local temp_file=$(mktemp)
    local output_file=$(mktemp)
    
    cat << 'EOF' > "$temp_file"
import ast
import sys
import re
from typing import Any, List, Dict, Set

def compare_types(hint: Any, doc: str) -> bool:
    """Compare type hint with docstring type."""
    if hint is Any or doc.lower() == "any":
        return True
        
    hint_str = str(hint).replace("typing.", "").lower()
    doc = doc.lower().strip()
    
    # Remove ", optional" from docstring type if present
    if ", optional" in doc:
        doc = doc.split(", optional")[0].strip()
    
    # Normalize whitespace and remove line breaks
    hint_str = ' '.join(hint_str.split())
    doc = ' '.join(doc.split())
    
    # handle common equivalences
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

def parse_raises_section(docstring: str) -> Dict[str, str]:
    """Parse the Raises section of a docstring, supporting NumPy, Google, and reST styles."""
    raises = {}
    if not docstring:
        return raises

    lines = [line.rstrip() for line in docstring.splitlines()]
    in_raises = False

    for i, line in enumerate(lines):
        stripped = line.strip()

        if re.match(r"^(Raises|Raises:)$", stripped, re.IGNORECASE):
            in_raises = True
            continue

        if in_raises:
            if not stripped:
                continue
            # End parsing if next section begins (e.g., Parameters, Returns, Notes, etc.)
            if re.match(r"^(Args|Arguments|Parameters|Returns|Yields|Notes|Examples|Attributes|See Also|References)(:)?$", stripped, re.IGNORECASE):
                break

            # Match formats like "ValueError: explanation"
            match = re.match(r"^([\w.]+)\s*:\s*(.*)", stripped)
            if match:
                exc, desc = match.groups()
                raises[exc.strip()] = desc.strip()
            else:
                # Match formats like "ValueError" (without colon)
                match = re.match(r"^([\w.]+)$", stripped)
                if match:
                    raises[match.group(1)] = ""

    return raises

def get_actual_raises(node: ast.AST) -> Set[str]:
    """Get all exceptions actually raised in the function."""
    raises = set()
    
    for n in ast.walk(node):
        if isinstance(n, ast.Raise):
            if n.exc is not None:
                # Handle direct exception names
                if isinstance(n.exc, ast.Name):
                    raises.add(n.exc.id)
                # Handle exception calls like ValueError("message")
                elif isinstance(n.exc, ast.Call) and isinstance(n.exc.func, ast.Name):
                    raises.add(n.exc.func.id)
                # Handle attribute access like exceptions.ValueError
                elif isinstance(n.exc, ast.Attribute):
                    raises.add(n.exc.attr)
                # Handle cases where exception is called directly
                elif isinstance(n.exc, ast.Call) and hasattr(n.exc.func, 'id'):
                    raises.add(n.exc.func.id)
    
    return raises

def normalize_exception_name(name: str) -> str:
    """Normalize exception names for comparison."""
    return name.split('.')[-1]  # Get just the exception name without module

def check_file(filepath: str) -> int:
    errors = 0
    with open(filepath, 'r') as f:
        tree = ast.parse(f.read(), filename=filepath)
    
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            lineno = node.lineno
            if not node.returns and not any(isinstance(decorator, ast.Name) and 
               decorator.id == 'property' for decorator in node.decorator_list):
                continue
                
            docstring = ast.get_docstring(node)
            if not docstring:
                print(f"⚠️  Missing docstring in {node.name}() at line {lineno}")
                errors += 1
                continue
                
            # check return type
            if node.returns:
                returns = ast.unparse(node.returns)
                if "Returns" in docstring or "returns" in docstring:
                    doc_lines = [l.rstrip() for l in docstring.split('\n')]
                    found_return = False
                    for i, line in enumerate(doc_lines):
                        if line.strip().lower() == "returns":
                            j = i + 1
                            while j < len(doc_lines) and set(doc_lines[j].strip()) <= {"-", " "}:
                                j += 1
                            while j < len(doc_lines):
                                candidate = doc_lines[j].strip()
                                if candidate:
                                    doc_type = candidate
                                    if not compare_types(returns, doc_type):
                                        print(f"❌ Return type mismatch in {node.name}() at line {lineno}:")
                                        print(f"   Type hint: {returns}")
                                        print(f"   Docstring: {doc_type}")
                                        errors += 1
                                    found_return = True
                                    break
                                j += 1
                            break
                    if not found_return:
                        print(f"⚠️  Return type documented but no type hint in {node.name}() at line {lineno}")
                        errors += 1
            
            # check parameters
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
                                print(f"❌ Parameter type mismatch in {node.name}({arg.arg}) at line {lineno}:")
                                print(f"   Type hint: {hint}")
                                print(f"   Docstring: {doc_type}")
                                errors += 1
                            arg_doc_found = True
                            break
                    if not arg_doc_found:
                        print(f"⚠️  Missing docstring for parameter {arg.arg} in {node.name}() at line {lineno}")
                        errors += 1
            
            # check Raises section
            doc_raises = parse_raises_section(docstring)
            actual_raises = get_actual_raises(node)
            
            # normalize all exception names for comparison
            doc_exceptions = {normalize_exception_name(e) for e in doc_raises}
            actual_exceptions = {normalize_exception_name(e) for e in actual_raises}
            
            # check for documented but not raised exceptions
            for exc in doc_exceptions:
                if exc not in actual_exceptions:
                    print(f"⚠️  Documented but not raised exception {exc} in {node.name}() at line {lineno}")
                    errors += 1
            
            # check for raised but not documented exceptions
            for exc in actual_exceptions:
                if exc not in doc_exceptions:
                    print(f"⚠️  Raised but not documented exception {exc} in {node.name}() at line {lineno}")
                    errors += 1
    
    return errors

if __name__ == "__main__":
    errors = check_file(sys.argv[1])
    sys.exit(1 if errors > 0 else 0)
EOF

    # run the check and capture output
    python "$temp_file" "$path" > "$output_file" 2>&1
    local exit_code=$?
    
    # print the output
    cat "$output_file"
    
    # check for any warnings or errors
    if grep -q -E "⚠️|❌" "$output_file"; then
        print_status "error" "Type hint/docstring inconsistencies found in ${type}"
        rm "$temp_file" "$output_file"
        return 1
    fi
    
    rm "$temp_file" "$output_file"
    
    if [ $exit_code -ne 0 ]; then
        print_status "error" "Type checking script failed for ${type}"
        return 1
    fi
    
    print_status "success" "Type hints and docstrings are consistent for ${type}"
    return 0
}

run_ruff() {
    local path="$1"
    local type="$2"
    print_status "info" "Running Ruff on ${type}: ${path}"
    if ! poetry run ruff check "$path" --config=ruff.toml; then
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