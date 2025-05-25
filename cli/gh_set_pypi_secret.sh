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

# check if a command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# authenticate with GitHub CLI using SSH
gh_authenticate_with_ssh() {
    print_status "info" "Attempting to authenticate with GitHub using SSH..."
    
    # Check if SSH agent is running
    if [ -z "$SSH_AUTH_SOCK" ]; then
        print_status "config" "Starting SSH agent..."
        eval "$(ssh-agent -s)"
    fi

    # Try to add default SSH key
    if [ -f "$HOME/.ssh/id_ed25519" ]; then
        ssh-add "$HOME/.ssh/id_ed25519" 2>/dev/null
    elif [ -f "$HOME/.ssh/id_rsa" ]; then
        ssh-add "$HOME/.ssh/id_rsa" 2>/dev/null
    fi

    # Perform the login
    print_status "config" "Logging into GitHub CLI with SSH..."
    if gh auth login --with-token < <(gh auth token 2>/dev/null) || \
       gh auth login -h github.com -p ssh -s admin:public_key -s write:public_key; then
        print_status "success" "Successfully authenticated with GitHub via SSH"
        return 0
    else
        print_status "error" "Failed to authenticate with GitHub via SSH"
        return 1
    fi
}

# verify github cli authentication
check_gh_authentication() {
    if ! gh auth status >/dev/null 2>&1; then
        print_status "warning" "Not logged into GitHub CLI"
        if ! gh_authenticate_with_ssh; then
            print_status "error" "Could not authenticate automatically"
            print_status "info" "Please run manually: gh auth login"
            exit 1
        fi
    fi
}

# extract value from .env file
get_env_value() {
    local key="$1"
    grep -E "^${key}=" .env | cut -d '=' -f2- | sed -e 's/^"//' -e 's/"$//'
}

# main function
main() {
    print_status "info" "Starting GitHub secrets setup"

    # check if .env file exists
    if [ ! -f ".env" ]; then
        print_status "error" ".env file not found in the current directory"
        exit 1
    fi

    # check if gh cli is installed
    if ! command_exists gh; then
        print_status "error" "GitHub CLI (gh) is not installed"
        print_status "info" "Install it from: https://cli.github.com/"
        exit 1
    fi

    # verify authentication
    check_gh_authentication

    # extract pypi_token from .env file
    print_status "config" "Looking for PYPI_TOKEN in .env file"
    PYPI_TOKEN=$(get_env_value "PYPI_TOKEN")

    if [ -z "$PYPI_TOKEN" ]; then
        print_status "error" "PYPI_TOKEN not found in .env file"
        exit 1
    fi

    # set the secret
    print_status "config" "Setting PYPI_TOKEN as GitHub secret..."
    if gh secret set PYPI_TOKEN --body "$PYPI_TOKEN"; then
        print_status "success" "GitHub secret PYPI_TOKEN has been set successfully"
    else
        print_status "error" "Failed to set GitHub secret"
        exit 1
    fi

    # verify the secret was set
    print_status "info" "Verifying secret was set..."
    if gh secret list | grep -q PYPI_TOKEN; then
        print_status "success" "Verification successful - PYPI_TOKEN is in the secrets list"
    else
        print_status "warning" "PYPI_TOKEN not found in secrets list (but may have been set)"
    fi
}

# execute main function
main