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

# function to generate ssh keys
generate_ssh_key() {
    # ask for user email
    print_status "info" "SSH Key Generation Process"
    read -p "$(echo -e "${CYAN}[→]${NC} Enter your email address: ")" email

    # validate email input
    if [[ -z "$email" ]]; then
        print_status "error" "Email address cannot be empty!"
        return 1
    fi

    # default key path
    key_path="$HOME/.ssh/id_ed25519"

    # ask if user wants to specify custom path
    read -p "$(echo -e "${CYAN}[→]${NC} Enter custom path for SSH key (leave empty for default $key_path): ")" custom_path

    # use custom path if provided
    if [[ -n "$custom_path" ]]; then
        key_path="$custom_path"
    fi

    # check if key already exists
    if [[ -f "$key_path" ]]; then
        print_status "warning" "SSH key already exists at $key_path"
        read -p "$(echo -e "${YELLOW}[!]${NC} Overwrite? (y/N): ")" overwrite
        if [[ ! "$overwrite" =~ ^[Yy]$ ]]; then
            print_status "info" "Key generation cancelled."
            return 0
        fi
    fi

    # generate the ssh key
    print_status "config" "Generating new ED25519 SSH key pair..."
    ssh-keygen -t ed25519 -C "$email" -f "$key_path"

    # check if key generation was successful
    if [ $? -eq 0 ]; then
        print_status "success" "SSH key pair generated successfully!"
        print_status "info" "Public key: ${key_path}.pub"
        
        # display public key
        echo -e "\n${CYAN}Your public key:${NC}"
        cat "${key_path}.pub"
        
        # add to ssh agent
        echo -e "\n"
        read -p "$(echo -e "${CYAN}[→]${NC} Add key to SSH agent? (Y/n): ")" add_to_agent
        if [[ ! "$add_to_agent" =~ ^[Nn]$ ]]; then
            eval "$(ssh-agent -s)" >/dev/null
            ssh-add "$key_path"
            print_status "success" "Key added to SSH agent!"
        fi
        
        # copy to clipboard if xclip is available
        if command -v xclip &> /dev/null; then
            xclip -sel clip < "${key_path}.pub"
            print_status "info" "Public key copied to clipboard!"
        else
            print_status "warning" "xclip not found. Install with 'sudo apt install xclip' to enable clipboard copy."
        fi
    else
        print_status "error" "Failed to generate SSH key pair!"
        return 1
    fi
}

generate_ssh_key