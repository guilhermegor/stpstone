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
    run_ruff "$module_path" "module" || return 1
    run_ruff "$test_path" "tests" || return 1
    run_pytest "$test_path" || return 1

    print_status "success" "=== All checks passed for ${module} ==="
}

main "$@"