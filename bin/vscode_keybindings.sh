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

VSCODE_USER_DIR="$HOME/.config/Code/User"
KEYBINDINGS_FILE="$VSCODE_USER_DIR/keybindings.json"
BACKUP_FILE="$VSCODE_USER_DIR/keybindings.backup.$(date +%Y%m%d-%H%M%S).json"

KEYBINDING='{
    "key": "ctrl+k s",
    "command": "workbench.action.files.saveAll",
    "when": "!inDebugMode"
}'

create_vscode_directory() {
    print_status "debug" "Checking VS Code user directory..."
    
    if [ ! -d "$VSCODE_USER_DIR" ]; then
        print_status "config" "Creating VS Code user directory: $VSCODE_USER_DIR"
        mkdir -p "$VSCODE_USER_DIR"
        
        if [ $? -eq 0 ]; then
            print_status "success" "Directory created successfully"
            return 0
        else
            print_status "error" "Failed to create directory"
            return 1
        fi
    else
        print_status "debug" "VS Code user directory already exists"
        return 0
    fi
}

backup_existing_keybindings() {
    if [ -f "$KEYBINDINGS_FILE" ]; then
        print_status "config" "Backing up existing keybindings to: $(basename "$BACKUP_FILE")"
        cp "$KEYBINDINGS_FILE" "$BACKUP_FILE"
        
        if [ $? -eq 0 ]; then
            print_status "success" "Backup created successfully"
            return 0
        else
            print_status "warning" "Failed to create backup, continuing anyway..."
            return 1
        fi
    else
        print_status "debug" "No existing keybindings file found"
        return 0
    fi
}

check_existing_keybinding() {
    if [ ! -f "$KEYBINDINGS_FILE" ]; then
        return 1
    fi
    
    print_status "config" "Checking for existing keybinding..."
    
    if grep -q "ctrl+k s" "$KEYBINDINGS_FILE"; then
        print_status "warning" "Keybinding 'Ctrl+K S' already exists!"
        print_status "info" "Checking if it's configured for 'saveAll' command..."
        
        if grep -A2 -B2 "ctrl+k s" "$KEYBINDINGS_FILE" | grep -q "workbench.action.files.saveAll"; then
            print_status "success" "Keybinding is already correctly configured"
            print_status "info" "No changes needed"
            return 0  # already configured correctly
        else
            print_status "warning" "Existing keybinding has different command"
            print_status "info" "You may want to manually review: $KEYBINDINGS_FILE"
            return 0  # exists but different - don't override
        fi
    fi
    
    return 1  # keybinding doesn't exist
}

validate_json_format() {
    local file="$1"
    
    if [ ! -f "$file" ]; then
        return 1
    fi
    
    print_status "debug" "Validating JSON format..."
    
    # try python3 first, then jq
    if command -v python3 > /dev/null 2>&1; then
        if python3 -m json.tool "$file" > /dev/null 2>&1; then
            return 0
        fi
    fi
    
    if command -v jq > /dev/null 2>&1; then
        if jq empty "$file" > /dev/null 2>&1; then
            return 0
        fi
    fi
    
    return 1
}

add_keybinding_to_existing_file() {
    print_status "config" "Processing existing keybindings file..."
    
    # validate existing JSON
    if ! validate_json_format "$KEYBINDINGS_FILE"; then
        print_status "error" "Existing keybindings.json has invalid JSON format"
        print_status "info" "Please fix the JSON syntax manually or delete the file to start fresh"
        return 1
    fi
    
    print_status "config" "Adding new keybinding to existing configuration..."
    
    # create temporary file with new keybinding
    local temp_file
    temp_file=$(mktemp)
    
    # remove last closing bracket, add comma and new keybinding, then close array
    sed '$ s/]//' "$KEYBINDINGS_FILE" > "$temp_file"
    echo ",$KEYBINDING" >> "$temp_file"
    echo "]" >> "$temp_file"
    
    # validate the new JSON
    if validate_json_format "$temp_file"; then
        mv "$temp_file" "$KEYBINDINGS_FILE"
        print_status "success" "Keybinding added to existing configuration"
        return 0
    else
        print_status "error" "Generated JSON is invalid, reverting changes..."
        rm "$temp_file"
        return 1
    fi
}

create_new_keybindings_file() {
    print_status "config" "Creating new keybindings.json file..."
    echo "[$KEYBINDING]" > "$KEYBINDINGS_FILE"
    
    if [ -f "$KEYBINDINGS_FILE" ]; then
        print_status "success" "New keybindings file created"
        return 0
    else
        print_status "error" "Failed to create keybindings file"
        return 1
    fi
}

validate_final_configuration() {
    if [ -f "$KEYBINDINGS_FILE" ] && grep -q "ctrl+k s" "$KEYBINDINGS_FILE"; then
        return 0
    else
        print_status "error" "Configuration failed - keybinding not found in file"
        return 1
    fi
}

display_success_message() {
    print_status "success" "VS Code keybinding configured successfully!"
    echo ""
    print_status "info" "${CYAN}Custom Keybinding Added:${NC}"
    print_status "config" "Ctrl+K, S → Save All Files"
    echo ""
    print_status "warning" "Please reload VS Code for changes to take effect:"
    print_status "config" "1. Press Ctrl+Shift+P"
    print_status "config" "2. Type 'Developer: Reload Window'"
    print_status "config" "3. Press Enter"
    echo ""
    print_status "info" "Alternatively, restart VS Code completely"
}

display_header() {
    print_status "info" "Setting up VS Code keybindings for this project..."
    echo ""
}


main() {
    display_header
    
    if ! create_vscode_directory; then
        exit 1
    fi
    
    backup_existing_keybindings
    
    if check_existing_keybinding; then
        exit 0
    fi
    
    if [ -f "$KEYBINDINGS_FILE" ]; then
        if ! add_keybinding_to_existing_file; then
            exit 1
        fi
    else
        if ! create_new_keybindings_file; then
            exit 1
        fi
    fi
    
    if ! validate_final_configuration; then
        exit 1
    fi
    
    display_success_message
    echo ""
    print_status "success" "Setup completed!"
}

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi