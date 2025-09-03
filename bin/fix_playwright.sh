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

fix_playwright_installation() {
    print_status "info" "Fixing Playwright installation..."

    # Clean up existing Playwright cache
    print_status "info" "Cleaning up existing Playwright browser cache..."
    rm -rf ~/.cache/ms-playwright/chromium* || true

    # Reinstall Playwright Python package to ensure latest version
    print_status "info" "Reinstalling Playwright Python package..."
    poetry run pip uninstall -y playwright || {
        print_status "warning" "Playwright package not found, proceeding with fresh installation"
    }
    poetry run pip install playwright || {
        print_status "error" "Failed to install Playwright Python package"
        return 1
    }

    # Install browsers with dependencies
    print_status "info" "Installing Playwright browsers..."
    poetry run playwright install chromium --with-deps || {
        print_status "error" "Failed to install Playwright Chromium browser"
        return 1
    }

    # Verify installation
    print_status "info" "Verifying Playwright installation..."
    if poetry run python -c "
from playwright.sync_api import sync_playwright
try:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        browser.close()
    print('Playwright working correctly')
except Exception as e:
    print(f'Error: {e}')
    exit(1)
"; then
        print_status "success" "Playwright installation verified successfully"
        return 0
    else
        print_status "error" "Playwright installation verification failed"
        return 1
    fi
}

main() {
    fix_playwright_installation || exit 1
}

main "$@"