#!/bin/bash
set -eo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
MAGENTA='\033[0;35m'
NC='\033[0m'

REQUIREMENTS_FILE="requirements-prd.txt"
REQUIREMENTS_DEV_FILE=".vscode/extensions.txt"
VSCODE_PREFIX="vscode:"

print_status() {
    local status="$1"
    local message="$2"
    case "$status" in
        "success") echo -e "${GREEN}[✓]${NC} ${message}" ;;
        "error") echo -e "${RED}[✗]${NC} ${message}" >&2 ;;
        "warning") echo -e "${YELLOW}[!]${NC} ${message}" >&2 ;;
        "info") echo -e "${BLUE}[i]${NC} ${message}" ;;
        "config") echo -e "${CYAN}[→]${NC} ${message}" ;;
        "debug") echo -e "${MAGENTA}[»]${NC} ${message}" ;;
        *) echo -e "[ ] ${message}" ;;
    esac
}

handle_error() {
    print_status "error" "$1"
    exit 1
}

check_command() {
    if ! command -v "$1" &> /dev/null; then
        handle_error "'$1' not found. Please install it first."
    fi
    print_status "success" "Found command: $1"
}

export_prod_dependencies() {
    print_status "info" "Exporting production dependencies to $REQUIREMENTS_FILE..."
    if ! poetry export --without dev --format requirements.txt --output "$REQUIREMENTS_FILE" --without-hashes; then
        print_status "warning" "Failed to export with poetry, falling back to pip freeze"
        if ! poetry run python -m pip freeze > "$REQUIREMENTS_FILE"; then
            handle_error "Failed to export production dependencies"
        fi
    fi
    print_status "success" "Production dependencies exported successfully"
}

export_vscode_extensions() {
    print_status "info" "Exporting VSCode extensions to $REQUIREMENTS_DEV_FILE..."
    if command -v code &> /dev/null; then
        code --list-extensions | awk -v prefix="$VSCODE_PREFIX" '{print prefix $0}' > "$REQUIREMENTS_DEV_FILE"
        print_status "success" "VSCode extensions exported successfully"
    else
        print_status "warning" "VSCode not found, creating empty extensions file"
        touch "$REQUIREMENTS_DEV_FILE"
    fi
}

main() {
    print_status "info" "Starting dependency export process"
    
    check_command "poetry"
    
    export_prod_dependencies
    export_vscode_extensions
    
    print_status "success" "Dependency export completed successfully"
    exit 0
}

main