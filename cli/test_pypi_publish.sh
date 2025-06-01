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

load_env() {
    local env_file=".env"
    
    if [[ -f "$env_file" ]]; then
        print_status "info" "Loading environment variables from $env_file"
        # export variables while preserving existing ones
        export $(grep -v '^#' "$env_file" | xargs)
    else
        print_status "error" "Environment file $env_file not found"
        exit 1
    fi
}

upload_to_testpypi() {
    print_status "info" "Building package with Poetry..."
    poetry build
    
    local dist_files="dist/*"
    if ! ls $dist_files >/dev/null 2>&1; then
        print_status "error" "No distribution files found in dist/"
        exit 1
    fi
    
    if [[ -z "$TEST_PYPI_TOKEN" ]]; then
        print_status "error" "TEST_PYPI_TOKEN is not set"
        print_status "info" "You can set it via:"
        print_status "info" "1. .env file (TEST_PYPI_TOKEN=your_token)"
        print_status "info" "2. Environment variable"
        print_status "info" "3. Poetry config (poetry config pypi-token.testpypi your_token)"
        exit 1
    fi
    
    print_status "config" "Uploading to TestPyPI using Poetry"
    print_status "debug" "Using token: ${TEST_PYPI_TOKEN:0:4}..."
    
    print_status "info" "Method 1: Using poetry publish (recommended)"
    poetry config pypi-token.testpypi "$TEST_PYPI_TOKEN"
    poetry publish -r testpypi --build
    
    if [[ $? -eq 0 ]]; then
        print_status "success" "Upload to TestPyPI completed successfully"
    else
        print_status "warning" "Poetry publish failed, trying twine as fallback..."
        
        # fallback to twine
        poetry run twine upload -r testpypi "$dist_files" \
            --verbose \
            --username "__token__" \
            --password "$TEST_PYPI_TOKEN"
        
        if [[ $? -eq 0 ]]; then
            print_status "success" "Twine upload to TestPyPI completed successfully"
        else
            print_status "error" "All upload attempts failed"
            exit 1
        fi
    fi
}

main() {
    load_env
    upload_to_testpypi
}

main "$@"