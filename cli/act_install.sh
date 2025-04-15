#!/bin/bash

# color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
# no color
NC='\033[0m'

check_act_installed() {
    if command -v act &> /dev/null; then
        echo -e "${GREEN}✓ act is installed: $(act --version)${NC}"
        return 0
    else
        return 1
    fi
}

install_linux() {
    echo -e "${CYAN}→ Linux detected. Installing act...${NC}"
    if ! command -v docker &> /dev/null; then
        echo -e "${YELLOW}⚠ Docker is required for act. Install it first:${NC}"
        echo -e "   https://docs.docker.com/engine/install/"
        return 1
    fi
    curl -s https://raw.githubusercontent.com/nektos/act/master/install.sh | sudo bash -s -- -b /usr/local/bin
}

install_macos() {
    echo -e "${CYAN}→ macOS detected. Installing act...${NC}"
    if ! command -v brew &> /dev/null; then
        echo -e "${YELLOW}⚠ Homebrew required. Installing it first...${NC}"
        /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
    fi
    brew install act
}

install_windows() {
    echo -e "${CYAN}→ Windows detected. Trying package managers...${NC}"
    if command -v winget &> /dev/null; then
        echo -e "${YELLOW}Trying winget...${NC}"
        winget install nektos.act -s winget && return 0
    fi
    if command -v choco &> /dev/null; then
        echo -e "${YELLOW}Trying Chocolatey...${NC}"
        choco install act-cli -y && return 0
    fi
    if command -v scoop &> /dev/null; then
        echo -e "${YELLOW}Trying Scoop...${NC}"
        scoop install act && return 0
    fi
    return 1
}

echo -e "\n=== ${CYAN}act (nektos/act) Installation Check${NC} ==="

if check_act_installed; then
    exit 0
fi

case "$(uname -s)" in
    Linux*)
        install_linux || INSTALL_FAILED=1
        ;;
    Darwin*)
        install_macos || INSTALL_FAILED=1
        ;;
    MINGW*|CYGWIN*|MSYS*)
        install_windows || INSTALL_FAILED=1
        ;;
    *)
        echo -e "${RED}✖ Unsupported OS: $(uname -s)${NC}"
        exit 1
        ;;
esac

if [ -z "$INSTALL_FAILED" ]; then
    if check_act_installed; then
        echo -e "\n${GREEN}✔ Success! Run ${CYAN}act --help${GREEN} to start.${NC}"
    else
        echo -e "\n${RED}✖ Installation succeeded but 'act' command not found!${NC}"
        echo -e "Try restarting your shell or check your PATH."
        exit 1
    fi
else
    echo -e "\n${RED}✖ Automatic installation failed!${NC}"
    echo -e "Please install manually:"
    case "$(uname -s)" in
        Linux*)
            echo -e "  ${YELLOW}curl -s https://raw.githubusercontent.com/nektos/act/master/install.sh | sudo bash${NC}"
            ;;
        Darwin*)
            echo -e "  ${YELLOW}brew install act${NC}"
            ;;
        MINGW*|CYGWIN*|MSYS*)
            echo -e "  Choose one:"
            echo -e "  • ${YELLOW}winget install nektos.act${NC} (Windows Package Manager)"
            echo -e "  • ${YELLOW}choco install act-cli${NC} (Chocolatey)"
            echo -e "  • ${YELLOW}scoop install act${NC} (Scoop)"
            ;;
    esac
    echo -e "Or download from: ${YELLOW}https://github.com/nektos/act/releases${NC}"
    exit 1
fi
