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

get_current_version() {
    poetry version -s
}

select_version_bump() {
    print_status "info" "Current version: $(get_current_version)"
    echo -e "${YELLOW}Select version bump type:${NC}"
    echo "1) Major (1.0.0 → 2.0.0)"
    echo "2) Minor (1.0.0 → 1.1.0)"
    echo "3) Patch (1.0.0 → 1.0.1)"
    echo "4) Custom version"
    echo -n "Your choice [1-4]: "
    
    read -r choice
    case $choice in
        1)
            poetry version major
            ;;
        2)
            poetry version minor
            ;;
        3)
            poetry version patch
            ;;
        4)
            echo -n "Enter custom version (e.g., 1.2.3): "
            read -r custom_version
            poetry version "$custom_version"
            ;;
        *)
            print_status "error" "Invalid selection"
            exit 1
            ;;
    esac
}

main() {
    print_status "info" "Starting version update process"
    
    if ! command -v poetry &> /dev/null; then
        print_status "error" "Poetry not found. Please install Poetry first."
        exit 1
    fi
    
    select_version_bump
    
    new_version=$(get_current_version)
    print_status "success" "New version: ${new_version}"
    
    print_status "info" "Updating dependencies..."
    poetry update
    
    print_status "success" "Version update complete!"
}

main