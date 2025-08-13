#!/bin/bash

check_gh_installed() {
    if command -v gh &> /dev/null; then
        echo "GitHub CLI (gh) is already installed."
        return 0
    else
        echo "GitHub CLI (gh) is not installed."
        return 1
    fi
}

install_gh() {
    local os_type
    os_type=$(uname -s)

    case "$os_type" in
        Linux*)
            if [[ -f /etc/os-release ]]; then
                source /etc/os-release
                case "$ID" in
                    debian|ubuntu|linuxmint)
                        echo "Detected Debian-based Linux. Installing gh..."
                        curl -fsSL https://cli.github.com/packages/githubcli-archive-keyring.gpg | sudo dd of=/usr/share/keyrings/githubcli-archive-keyring.gpg
                        echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/githubcli-archive-keyring.gpg] https://cli.github.com/packages stable main" | sudo tee /etc/apt/sources.list.d/github-cli.list > /dev/null
                        sudo apt update
                        sudo apt install gh -y
                        ;;
                    fedora|centos|rhel)
                        echo "Detected RPM-based Linux. Installing gh..."
                        sudo dnf config-manager --add-repo https://cli.github.com/packages/rpm/gh-cli.repo
                        sudo dnf install gh -y
                        ;;
                    arch|manjaro)
                        echo "Detected Arch Linux. Installing gh..."
                        sudo pacman -S github-cli --noconfirm
                        ;;
                    *)
                        echo "Unsupported Linux distribution. Please install gh manually: https://cli.github.com/"
                        return 1
                        ;;
                esac
            else
                echo "Could not determine Linux distribution. Please install gh manually: https://cli.github.com/"
                return 1
            fi
            ;;
        Darwin*)
            echo "Detected macOS. Installing gh..."
            if command -v brew &> /dev/null; then
                brew install gh
            else
                echo "Homebrew not found. Please install Homebrew first or download gh manually: https://cli.github.com/"
                return 1
            fi
            ;;
        MINGW*|CYGWIN*|MSYS*)
            echo "Detected Windows (Git Bash/Cygwin/MSYS)."
            if command -v winget &> /dev/null; then
                echo "Installing gh using winget..."
                winget install --id GitHub.cli
            elif command -v choco &> /dev/null; then
                echo "Installing gh using Chocolatey..."
                choco install gh
            elif command -v scoop &> /dev/null; then
                echo "Installing gh using Scoop..."
                scoop install gh
            else
                echo "No supported package manager found. Please install gh manually: https://cli.github.com/"
                return 1
            fi
            ;;
        *)
            echo "Unsupported operating system: $os_type"
            echo "Please install gh manually: https://cli.github.com/"
            return 1
            ;;
    esac

    if command -v gh &> /dev/null; then
        echo "GitHub CLI (gh) installed successfully!"
        return 0
    else
        echo "Installation failed. Please install gh manually: https://cli.github.com/"
        return 1
    fi
}

if ! check_gh_installed; then
    echo "Attempting to install GitHub CLI..."
    if install_gh; then
        echo "Installation successful. You may need to restart your terminal."
    else
        echo "Failed to install GitHub CLI. Please install it manually."
        exit 1
    fi
fi

echo "Checking GitHub authentication status..."
if gh auth status &> /dev/null; then
    echo "Already authenticated with GitHub."
else
    echo "You need to authenticate with GitHub. Running 'gh auth login'..."
    gh auth login
fi

echo "GitHub CLI is ready to use!"
