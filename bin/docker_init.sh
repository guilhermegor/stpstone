#!/bin/bash

# Color definitions
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
MAGENTA='\033[0;35m'
NC='\033[0m' # No color

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

check_docker_accessible() {
    if docker info >/dev/null 2>&1; then
        print_status "success" "Docker is accessible"
        return 0
    else
        print_status "error" "Docker is running but not accessible to current user"
        return 1
    fi
}

fix_docker_permissions() {
    print_status "info" "Attempting to fix Docker permissions..."
    
    # Add user to docker group if not already
    if ! groups | grep -q '\bdocker\b'; then
        sudo usermod -aG docker $USER
        print_status "info" "Added user to docker group"
        
        # Try to apply group changes without logout
        print_status "info" "Attempting to apply group changes without logout..."
        if command -v newgrp &>/dev/null; then
            exec newgrp docker <<EONG
            print_status "info" "Running in new shell with docker group permissions"
            exit 0
EONG
        else
            print_status "warning" "'newgrp' not available - please log out and back in"
            return 1
        fi
    fi
    
    # Restart docker service
    sudo systemctl restart docker
    
    # Verify
    if check_docker_accessible; then
        print_status "success" "Docker permissions fixed"
        return 0
    else
        print_status "error" "Could not fix Docker permissions automatically"
        return 1
    fi
}

test_docker_connection() {
    print_status "debug" "Testing Docker connection with hello-world..."
    if docker run --rm hello-world >/dev/null 2>&1; then
        print_status "success" "Docker connection test passed"
        return 0
    else
        print_status "error" "Docker connection test failed"
        return 1
    fi
}

main() {
    print_status "info" "Starting Docker initialization..."
    
    # Check if Docker is installed
    if ! command -v docker &>/dev/null; then
        print_status "error" "Docker is not installed. Please install Docker first."
        exit 1
    fi
    
    # Check if Docker is accessible
    if ! check_docker_accessible; then
        print_status "warning" "Docker is not accessible - attempting to fix..."
        if ! fix_docker_permissions; then
            print_status "error" "Please either:"
            print_status "error" "1. Log out and back in, or"
            print_status "error" "2. Run this command in your shell: newgrp docker"
            exit 1
        fi
    fi
    
    # Final test
    if ! test_docker_connection; then
        print_status "error" "Docker initialization failed"
        exit 1
    fi
    
    print_status "success" "Docker is ready"
}

main