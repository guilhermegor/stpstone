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

command_exists() {
    local cmd="$1"
    if ! command -v "$cmd" &> /dev/null; then
        print_status "error" "Command not found: ${cmd}"
        return 1
    fi
    return 0
}

check_requirements() {
    local required_commands=("code" "sed" "grep")
    local missing_commands=()
    
    for cmd in "${required_commands[@]}"; do
        if ! command_exists "$cmd"; then
            missing_commands+=("$cmd")
        fi
    done

    if [ ${#missing_commands[@]} -gt 0 ]; then
        print_status "error" "The following required commands are missing: ${missing_commands[*]}"
        
        if [[ " ${missing_commands[*]} " == *" code "* ]]; then
            print_status "info" "To install VS Code CLI, you need to:"
            print_status "info" "1. Open VS Code"
            print_status "info" "2. Open the Command Palette (Ctrl+Shift+P or Cmd+Shift+P)"
            print_status "info" "3. Search for 'Shell Command'"
            print_status "info" "4. Select 'Install 'code' command in PATH'"
        fi
        
        return 1
    fi
    
    return 0
}

normalize_line_endings() {
    local file="$1"
    print_status "info" "Normalizing line endings in ${file}"
    if sed -i 's/\r//g' "$file"; then
        print_status "success" "Line endings normalized successfully"
    else
        print_status "error" "Failed to normalize line endings"
        return 1
    fi
}

install_vscode_extensions() {
    local file="$1"
    print_status "info" "Starting VSCode extensions installation from ${file}"
    
    if [ ! -f "$file" ]; then
        print_status "error" "Extensions file not found: ${file}"
        return 1
    fi

    local count=0
    local total=0
    total=$(grep -c '^vscode:' "$file")
    
    print_status "info" "Found ${total} extensions to install"
    
    grep '^vscode:' "$file" | while IFS= read -r extension; do
        ext_id="${extension#vscode:}"
        print_status "config" "Installing extension: ${ext_id}"
        if code --install-extension "${ext_id}"; then
            print_status "success" "Successfully installed: ${ext_id}"
            ((count++))
        else
            print_status "error" "Failed to install: ${ext_id}"
        fi
    done

    print_status "info" "Installation complete. ${count} of ${total} extensions were successfully installed."
    
    if [ "$count" -lt "$total" ]; then
        print_status "warning" "Some extensions failed to install"
        return 1
    fi
    
    return 0
}

main() {
    local extensions_file=".vscode/extensions.txt"
    
    if ! check_requirements; then
        print_status "error" "System requirements not met. Aborting."
        exit 1
    fi
    
    if ! normalize_line_endings "$extensions_file"; then
        print_status "error" "Aborting due to previous errors"
        exit 1
    fi
    
    if ! install_vscode_extensions "$extensions_file"; then
        print_status "warning" "Some extensions failed to install"
        exit 1
    fi
    
    print_status "success" "All operations completed successfully"
}

main