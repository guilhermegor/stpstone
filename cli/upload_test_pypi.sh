#!/bin/bash

# color definitions
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
MAGENTA='\033[0;35m'
NC='\033[0m' # no color

# print colored status messages
print_status() {
    local status="$1"
    local message="$2"
    case "$status" in
        "success")
            echo -e "${GREEN}[✓]${NC} ${message}"
            ;;
        "error")
            echo -e "${RED}[✗]${NC} ${message}" >&2
            ;;
        "warning")
            echo -e "${YELLOW}[!]${NC} ${message}"
            ;;
        "info")
            echo -e "${BLUE}[i]${NC} ${message}"
            ;;
        "config")
            echo -e "${CYAN}[→]${NC} ${message}"
            ;;
        "debug")
            echo -e "${MAGENTA}[»]${NC} ${message}"
            ;;
        *)
            echo -e "[ ] ${message}"
            ;;
    esac
}

# function to load environment variables from .env file
load_env() {
    local env_file=".env"
    
    if [[ -f "$env_file" ]]; then
        print_status "info" "Loading environment variables from $env_file"
        # Export variables while preserving existing ones
        export $(grep -v '^#' "$env_file" | xargs)
    else
        print_status "error" "Environment file $env_file not found"
        exit 1
    fi
}

# function to upload to test pypi
upload_to_testpypi() {
    local dist_files="dist/*"
    
    # check if distribution files exist
    if ! ls $dist_files >/dev/null 2>&1; then
        print_status "error" "No distribution files found in dist/"
        exit 1
    fi
    
    # check if test_pypi_token is set
    if [[ -z "$TEST_PYPI_TOKEN" ]]; then
        print_status "error" "TEST_PYPI_TOKEN is not set in .env file"
        exit 1
    fi
    
    print_status "config" "Uploading to TestPyPI with token from .env"
    print_status "debug" "Using token: ${TEST_PYPI_TOKEN:0:4}..."
    
    # execute the upload command
    twine upload -r testpypi "$dist_files" --verbose --username "__token__" --password "$TEST_PYPI_TOKEN"
    
    if [[ $? -eq 0 ]]; then
        print_status "success" "Upload to TestPyPI completed successfully"
    else
        print_status "error" "Upload to TestPyPI failed"
        exit 1
    fi
}

# main function
main() {
    load_env
    upload_to_testpypi
}

# run main function
main "$@"