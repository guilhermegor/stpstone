#!/bin/bash

declare -a EXCLUDE_PATTERNS=(
    "tests/unit/*"
    "tests/integration/*"
    "tests/performance/*"
    "stpstone/utils/parsers/mock_exclusion_example.py"
)

# list of classes considered to have TypeChecker
declare -a TYPECHECKER_CLASSES=(
    "TypeChecker"
    "ABCSession"
    "ABCDatabase"
    "ABCCalendarOperations"
)

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

detect_os() {
    case "$(uname -s)" in
        Linux*)     echo "linux" ;;
        Darwin*)    echo "macos" ;;
        CYGWIN*|MINGW*|MSYS*) echo "windows" ;;
        *)          echo "unknown" ;;
    esac
}

check_command_exists() {
    command -v "$1" >/dev/null 2>&1
}

check_library_exists() {
    local lib_name="$1"
    case "$(detect_os)" in
        "linux")
            ldconfig -p | grep -q "$lib_name" 2>/dev/null || 
            find /usr/lib* /lib* -name "*${lib_name}*" 2>/dev/null | head -1 | grep -q .
            ;;
        "macos")
            find /usr/lib /usr/local/lib /opt/homebrew/lib -name "*${lib_name}*" 2>/dev/null | head -1 | grep -q .
            ;;
        "windows")
            # On Windows, check for ODBC drivers in system directories
            find /c/Windows/System32 /c/Windows/SysWOW64 -name "*odbc*" 2>/dev/null | head -1 | grep -q . ||
            reg query "HKEY_LOCAL_MACHINE\SOFTWARE\ODBC\ODBCINST.INI" 2>/dev/null | grep -q "ODBC"
            ;;
        *)
            return 1
            ;;
    esac
}

install_odbc_linux() {
    print_status "info" "Installing ODBC drivers for Linux..."
    
    # Check if we have sudo privileges
    if ! sudo -n true 2>/dev/null; then
        print_status "error" "sudo privileges required to install ODBC drivers"
        return 1
    fi
    
    # Detect distribution
    if [ -f /etc/debian_version ]; then
        # Debian/Ubuntu
        print_status "info" "Detected Debian/Ubuntu system"
        sudo apt update && sudo apt install -y unixodbc unixodbc-dev
    elif [ -f /etc/redhat-release ] || [ -f /etc/centos-release ]; then
        # RHEL/CentOS/Fedora
        print_status "info" "Detected RHEL/CentOS/Fedora system"
        if check_command_exists "dnf"; then
            sudo dnf install -y unixODBC unixODBC-devel
        elif check_command_exists "yum"; then
            sudo yum install -y unixODBC unixODBC-devel
        else
            print_status "error" "Neither dnf nor yum found"
            return 1
        fi
    elif [ -f /etc/arch-release ]; then
        # Arch Linux
        print_status "info" "Detected Arch Linux system"
        sudo pacman -S --noconfirm unixodbc
    else
        print_status "warning" "Unknown Linux distribution, trying generic approach"
        sudo apt update && sudo apt install -y unixodbc unixodbc-dev || {
            print_status "error" "Failed to install ODBC drivers. Please install manually."
            return 1
        }
    fi
}

install_odbc_macos() {
    print_status "info" "Installing ODBC drivers for macOS..."
    
    if check_command_exists "brew"; then
        brew install unixodbc
    else
        print_status "error" "Homebrew not found. Please install Homebrew first:"
        print_status "info" "/bin/bash -c \"\$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)\""
        return 1
    fi
}

install_odbc_windows() {
    print_status "info" "Checking ODBC drivers for Windows..."
    
    # Windows typically has ODBC drivers pre-installed
    if check_library_exists "odbc"; then
        print_status "success" "ODBC drivers already available on Windows"
        return 0
    fi
    
    print_status "warning" "ODBC drivers not detected on Windows"
    print_status "info" "Please install Microsoft ODBC drivers manually:"
    print_status "info" "1. Download from: https://docs.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server"
    print_status "info" "2. Or use winget: winget install Microsoft.SQLServerODBCDriver"
    return 1
}

check_and_install_odbc() {
    print_status "info" "Checking ODBC driver installation..."
    
    local os_type
    os_type=$(detect_os)
    print_status "debug" "Detected OS: ${os_type}"
    
    # Check if ODBC is already installed
    local odbc_installed=false
    
    case "$os_type" in
        "linux")
            if check_library_exists "libodbc.so" || check_command_exists "odbcinst"; then
                odbc_installed=true
            fi
            ;;
        "macos")
            if check_library_exists "libodbc" || check_command_exists "odbcinst"; then
                odbc_installed=true
            fi
            ;;
        "windows")
            if check_library_exists "odbc"; then
                odbc_installed=true
            fi
            ;;
        *)
            print_status "error" "Unsupported operating system: ${os_type}"
            return 1
            ;;
    esac
    
    if $odbc_installed; then
        print_status "success" "ODBC drivers already installed"
        
        # Additional verification by checking odbcinst command
        if check_command_exists "odbcinst"; then
            print_status "debug" "ODBC configuration:"
            odbcinst -j 2>/dev/null || print_status "warning" "Could not retrieve ODBC configuration"
        fi
        
        return 0
    fi
    
    print_status "warning" "ODBC drivers not found, attempting installation..."
    
    case "$os_type" in
        "linux")
            install_odbc_linux
            ;;
        "macos")
            install_odbc_macos
            ;;
        "windows")
            install_odbc_windows
            ;;
        *)
            print_status "error" "Cannot install ODBC drivers for unsupported OS: ${os_type}"
            return 1
            ;;
    esac
    
    local install_result=$?
    
    if [ $install_result -eq 0 ]; then
        print_status "success" "ODBC drivers installed successfully"
        
        # Verify installation
        if check_command_exists "odbcinst"; then
            print_status "info" "ODBC configuration after installation:"
            odbcinst -j 2>/dev/null || print_status "warning" "Could not retrieve ODBC configuration"
        fi
    else
        print_status "error" "Failed to install ODBC drivers"
        return 1
    fi
    
    return $install_result
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
    echo "$test_path"
}

install_playwright() {
    print_status "info" "Checking playwright..."
    if [ ! -d "$HOME/.cache/ms-playwright" ]; then
        print_status "info" "Installing Playwright browsers..."
        poetry run playwright install
    fi
    print_status "success" "Playwright installed"
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
    
    # convert arrays to Python-compatible strings
    exclude_patterns=$(printf '%s\n' "${EXCLUDE_PATTERNS[@]}" | python -c "import sys; print([line.strip() for line in sys.stdin])")
    typechecker_classes=$(printf '%s\n' "${TYPECHECKER_CLASSES[@]}" | python -c "import sys; print([line.strip() for line in sys.stdin])")
    
    cat << EOF > "$temp_file"
import ast
import sys
import re
import fnmatch
from typing import Any, List, Dict, Set, TypedDict

EXCLUDE_PATTERNS = $exclude_patterns
TYPECHECKER_CLASSES = $typechecker_classes

def should_exclude(filepath: str) -> bool:
    """Check if the file path matches any of the exclusion patterns."""
    for pattern in EXCLUDE_PATTERNS:
        if fnmatch.fnmatch(filepath, pattern):
            return True
    return False

def compare_types(hint: Any, doc: str) -> bool:
    """Compare type hint with docstring type."""
    if hint is Any or doc.lower() == "any":
        return True
        
    hint_str = str(hint).replace("typing.", "").lower()
    doc = doc.lower().strip()
    
    # remove ", optional" from docstring type if present
    if ", optional" in doc:
        doc = doc.split(", optional")[0].strip()
    
    # normalize whitespace and remove line breaks
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

def parse_numpy_parameters(docstring: str) -> Dict[str, str]:
    """Parse NumPy-style Parameters section."""
    params = {}
    if not docstring:
        return params
    
    lines = [line.rstrip() for line in docstring.splitlines()]
    in_params = False
    current_param = None
    
    for i, line in enumerate(lines):
        stripped = line.strip()
        
        # check for Parameters section
        if re.match(r"^Parameters:?$", stripped, re.IGNORECASE):
            in_params = True
            continue
        
        if in_params:
            # end if we hit another section
            if re.match(r"^(Returns?|Yields?|Raises?|Notes?|Examples?|Attributes?|See Also|References?)(:)?$", stripped, re.IGNORECASE):
                break
            
            # skip separator lines (dashes)
            if set(stripped) <= {"-", " "}:
                continue
            
            # empty line - continue to next parameter
            if not stripped:
                current_param = None
                continue
                
            # check if this is a parameter name line (not indented or starts with word : type pattern)
            if not line.startswith(' ') and line.strip():
                # Look for pattern "param_name : type"
                if ' : ' in stripped:
                    param_name, param_type = stripped.split(' : ', 1)
                    params[param_name.strip()] = param_type.strip()
                    current_param = None
                else:
                    # Just parameter name, type might be on next line
                    current_param = stripped
            elif current_param and line.startswith(' ') and ' : ' in stripped:
                # This is the type line for the previous parameter
                param_type = stripped.split(' : ', 1)[1].strip()
                params[current_param] = param_type
                current_param = None
    
    return params

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
            # end parsing if next section begins (e.g., Parameters, Returns, Notes, etc.)
            if re.match(r"^(Args|Arguments|Parameters|Returns|Yields|Notes|Examples|Attributes|See Also|References)(:)?$", stripped, re.IGNORECASE):
                break

            # match formats like "ValueError: explanation"
            match = re.match(r"^([\w.]+)\s*:\s*(.*)", stripped)
            if match:
                exc, desc = match.groups()
                raises[exc.strip()] = desc.strip()
            else:
                # match formats like "ValueError" (without colon)
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
                # handle direct exception names
                if isinstance(n.exc, ast.Name):
                    raises.add(n.exc.id)
                # handle exception calls like ValueError("message")
                elif isinstance(n.exc, ast.Call) and isinstance(n.exc.func, ast.Name):
                    raises.add(n.exc.func.id)
                # handle attribute access like exceptions.ValueError
                elif isinstance(n.exc, ast.Attribute):
                    raises.add(n.exc.attr)
                # handle cases where exception is called directly
                elif isinstance(n.exc, ast.Call) and hasattr(n.exc.func, 'id'):
                    raises.add(n.exc.func.id)
    
    return raises

def normalize_exception_name(name: str) -> str:
    """Normalize exception names for comparison."""
    return name.split('.')[-1]  # Get just the exception name without module

def check_type_checker_usage(node: ast.AST, filepath: str) -> int:
    """Check for TypeChecker metaclass and type_checker decorator usage."""
    errors = 0
    
    # skip TypeChecker checks for excluded paths
    if should_exclude(filepath):
        print(f"ℹ️  Skipping TypeChecker checks for excluded path: {filepath}")
        return 0
    
    tree = ast.parse(open(filepath).read(), filename=filepath)
    
    # build a map of classes and their bases
    class_bases = {}
    class_nodes = {}
    for n in ast.walk(tree):
        if isinstance(n, ast.ClassDef):
            class_bases[n.name] = [ast.unparse(base) for base in n.bases]
            class_nodes[n.name] = n

    # check if classes inherit from TYPECHECKER_CLASSES
    has_typechecker_inheritance = False
    for class_name, bases in class_bases.items():
        inherited_typechecker = any(
            base in TYPECHECKER_CLASSES or
            any(tc in base for tc in TYPECHECKER_CLASSES)
            for base in bases
        )
        if inherited_typechecker:
            print(f"ℹ️  Class {class_name} inherits from TypeChecker class: {', '.join(b for b in bases if b in TYPECHECKER_CLASSES or any(tc in b for tc in TYPECHECKER_CLASSES))}")
            has_typechecker_inheritance = True
        else:
            print(f"ℹ️  Class {class_name} does not inherit from any TypeChecker classes")

    # check for TypeChecker import
    has_type_checker_import = False
    for n in ast.walk(tree):
        if isinstance(n, ast.ImportFrom):
            if n.module == 'stpstone.transformations.validation.metaclass_type_checker':
                for alias in n.names:
                    if alias.name == 'TypeChecker' or alias.name == 'type_checker':
                        has_type_checker_import = True
                        break

    # Check if this is a class file (contains class definitions)
    has_classes = any(isinstance(n, ast.ClassDef) for n in ast.walk(tree))
    
    # For class files, either TypeChecker import or inheritance from TYPECHECKER_CLASSES is required
    if has_classes and not (has_type_checker_import or has_typechecker_inheritance):
        print(f"❌ Missing required TypeChecker import and no inheritance from TYPECHECKER_CLASSES in {filepath}")
        errors += 1
        return errors

    def has_type_checker_metaclass(class_node, visited=None):
        """Recursively check if a class or its bases have TypeChecker metaclass or inherit TypeChecker."""
        if visited is None:
            visited = set()
        
        # avoid infinite recursion
        if class_node.name in visited:
            return False
        visited.add(class_node.name)
        
        # check metaclass directly
        for keyword in class_node.keywords:
            if keyword.arg == 'metaclass':
                if (isinstance(keyword.value, ast.Name) and keyword.value.id == 'TypeChecker') or \
                   (isinstance(keyword.value, ast.Attribute) and keyword.value.attr == 'TypeChecker') or \
                   (isinstance(keyword.value, ast.Name) and keyword.value.id in class_nodes and \
                    'TypeChecker' in class_bases.get(keyword.value.id, [])):
                    return True
        
        # check base classes
        for base in class_node.bases:
            base_name = ast.unparse(base)
            # Check if base class inherits from TypeChecker or TYPECHECKER_CLASSES
            if base_name in TYPECHECKER_CLASSES or \
               base_name == 'TypeChecker' or \
               (isinstance(base, ast.Name) and base.id in TYPECHECKER_CLASSES) or \
               (isinstance(base, ast.Attribute) and base.attr in TYPECHECKER_CLASSES) or \
               (isinstance(base, ast.Name) and base.id == 'TypeChecker') or \
               (isinstance(base, ast.Attribute) and base.attr == 'TypeChecker'):
                return True
            # Find the base class node
            if base_name in class_nodes:
                base_node = class_nodes[base_name]
                if has_type_checker_metaclass(base_node, visited):
                    return True
        
        return False

    for n in ast.walk(tree):
        # check classes for TypeChecker metaclass
        if isinstance(n, ast.ClassDef):
            # skip TypedDict classes
            is_typed_dict = False
            for base in n.bases:
                if isinstance(base, ast.Name) and base.id == 'TypedDict':
                    is_typed_dict = True
                    break
                elif isinstance(base, ast.Attribute) and base.attr == 'TypedDict':
                    is_typed_dict = True
                    break
            if is_typed_dict:
                continue
                
            # check if class or its bases have TypeChecker or a class that inherits TypeChecker
            if not has_type_checker_metaclass(n):
                print(f"❌ Class {n.name} does not use TypeChecker metaclass or inherit from a TypeChecker-based class")
                errors += 1
        
        # check functions for type_checker decorator, but only if not in a class with TypeChecker
        elif isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)):
            # find the parent class, if any
            parent_class = None
            for parent in ast.walk(tree):
                if isinstance(parent, ast.ClassDef):
                    # Check if the function is a direct child of the class body
                    if any(child is n for child in parent.body):
                        parent_class = parent
                        break
            
            # if the function is in a class, check if the class or its bases have TypeChecker
            if parent_class and has_type_checker_metaclass(parent_class):
                continue  # Skip decorator check for methods in TypeChecker classes
            
            # check for type_checker decorator only for non-class methods or classes without TypeChecker
            has_decorator = False
            for decorator in n.decorator_list:
                if isinstance(decorator, ast.Name) and decorator.id == 'type_checker':
                    has_decorator = True
                    break
                elif isinstance(decorator, ast.Attribute) and decorator.attr == 'type_checker':
                    has_decorator = True
                    break
            
            if not has_decorator and not n.name.startswith('_'):
                print(f"❌ Function {n.name}() is not decorated with @type_checker")
                errors += 1
    
    return errors

def check_file(filepath: str) -> int:
    errors = 0
    with open(filepath, 'r') as f:
        tree = ast.parse(f.read(), filename=filepath)
    
    # first check type checker usage
    type_checker_errors = check_type_checker_usage(tree, filepath)
    errors += type_checker_errors
    
    if type_checker_errors > 0:
        print("❌ TypeChecker usage issues found. See errors above.")
        return errors
    
    # then check type/docstring consistency
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            lineno = node.lineno
            if not node.returns and not any(isinstance(decorator, ast.Name) and 
               decorator.id == 'property' for decorator in node.decorator_list):
                continue
                
            docstring = ast.get_docstring(node)
            if not docstring:
                print(f"❌ Missing docstring in {node.name}() at line {lineno}")
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
                        print(f"❌ Return type documented but no type hint in {node.name}() at line {lineno}")
                        errors += 1
            
            # check parameters using improved NumPy parser
            doc_params = parse_numpy_parameters(docstring)
            for arg in node.args.args:
                if arg.arg == 'self':
                    continue
                    
                if arg.annotation:
                    hint = ast.unparse(arg.annotation)
                    if arg.arg in doc_params:
                        doc_type = doc_params[arg.arg]
                        if not compare_types(hint, doc_type):
                            print(f"❌ Parameter type mismatch in {node.name}({arg.arg}) at line {lineno}:")
                            print(f"   Type hint: {hint}")
                            print(f"   Docstring: {doc_type}")
                            errors += 1
                    else:
                        print(f"❌ Missing docstring for parameter {arg.arg} in {node.name}() at line {lineno}")
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
                    print(f"❌ Documented but not raised exception {exc} in {node.name}() at line {lineno}")
                    errors += 1
            
            # check for raised but not documented exceptions
            for exc in actual_exceptions:
                if exc not in doc_exceptions:
                    print(f"❌ Raised but not documented exception {exc} in {node.name}() at line {lineno}")
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
    
    # check for any errors
    if grep -q "❌" "$output_file"; then
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
    print_status "config" "Classes considered to have TypeChecker: ${TYPECHECKER_CLASSES[*]}"

    local module_path
    module_path=$(find_module_path "$module") || exit 1
    local test_path
    test_path=$(find_test_path "$module")

    # Check and install system dependencies
    check_and_install_odbc || {
        print_status "warning" "ODBC installation failed, but continuing with tests..."
    }
    
    # Check Python dependencies
    install_playwright

    # run checks for module
    run_codespell "$module_path" "module" || return 1
    check_type_consistency "$module_path" "module" || return 1
    run_ruff "$module_path" "module" || return 1

    # run checks for test file if it exists
    if [ -n "$test_path" ]; then
        run_codespell "$test_path" "tests" || return 1
        check_type_consistency "$test_path" "tests" || return 1
        run_ruff "$test_path" "tests" || return 1
        run_pytest "$test_path" || return 1
    else
        print_status "warning" "No test file found for ${module}, skipping test-related checks"
    fi

    print_status "success" "=== All checks passed for ${module} ==="
}

main "$@"