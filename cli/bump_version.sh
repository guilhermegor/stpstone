#!/bin/bash

# Color definitions
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
MAGENTA='\033[0;35m'
NC='\033[0m' # No color

# Print colored status messages
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

# Get current version from pyproject.toml
get_current_version() {
    poetry version -s
}

# Update setup.py version
update_setup_py() {
    local new_version=$1
    if [ -f setup.py ]; then
        sed -i "s/version=['\"][^'\"]*['\"]/version='${new_version}'/" setup.py
        print_status "success" "Updated setup.py to version ${new_version}"
    else
        print_status "warning" "setup.py not found - skipping update"
    fi
}

# Interactive version bump selection
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

# Main workflow
main() {
    print_status "info" "Starting version update process"
    
    # Verify poetry is available
    if ! command -v poetry &> /dev/null; then
        print_status "error" "Poetry not found. Please install Poetry first."
        exit 1
    fi
    
    # Select and apply version bump
    select_version_bump
    
    # Get new version
    new_version=$(get_current_version)
    print_status "success" "New version: ${new_version}"
    
    # Update dependencies
    print_status "info" "Updating dependencies..."
    poetry update
    
    # Update setup.py if exists
    update_setup_py "$new_version"
    
    print_status "success" "Version update complete!"
}

# Execute main function
main