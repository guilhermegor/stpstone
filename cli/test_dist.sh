#!/bin/bash
# Color definitions
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
MAGENTA='\033[0;35m'
NC='\033[0m' # no color

# Print colored status messages
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

# Get the repository/package name from current directory
get_package_name() {
    local repo_name=$(basename "$(pwd)")
    local package_name="${repo_name//-/_}" # Convert dashes to underscores
    print_status "config" "Detected package name: ${MAGENTA}${package_name}${NC}"
    echo "$package_name"
}

# Check if package is installed and install if needed
check_and_install() {
    local package_name=$1
    
    # Create a temporary Python script to avoid string interpolation issues
    cat << 'EOF' > /tmp/check_install.py
import importlib.util
import sys
import subprocess

def check_and_install_package(package_name):
    spec = importlib.util.find_spec(package_name)
    if spec is None:
        print(f'{package_name} not installed. Installing now...')
        subprocess.run(['make', 'install_dist_locally'], check=True)
    else:
        try:
            import pkg_resources
            current_version = pkg_resources.get_distribution(package_name).version
            print(f'{package_name} version {current_version} is already installed.')
            answer = input('Do you want to reinstall? [y/N]: ').strip().lower()
            if answer == 'y':
                print('Reinstalling...')
                subprocess.run(['make', 'install_dist_locally'], check=True)
        except pkg_resources.DistributionNotFound:
            print(f'{package_name} not properly installed. Installing now...')
            subprocess.run(['make', 'install_dist_locally'], check=True)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python check_install.py <package_name>")
        sys.exit(1)
    check_and_install_package(sys.argv[1])
EOF
    
    python3 /tmp/check_install.py "$package_name"
    rm -f /tmp/check_install.py
}

# Run twine check on distribution files
run_twine_check() {
    print_status "info" "Running twine check on distribution files..."
    if twine check dist/*; then
        print_status "success" "Twine validation passed"
    else
        print_status "error" "Twine check failed"
        return 1
    fi
}

# Run automated unit and integration tests
run_automated_tests() {
    local test_failed=0
    
    print_status "info" "Running automated tests..."
    
    # Run unit tests
    if [ -d "tests/unit" ]; then
        print_status "info" "Running unit tests..."
        if python -m unittest discover -s tests/unit -p "*.py" -v; then
            print_status "success" "Unit tests passed"
        else
            print_status "error" "Unit tests failed"
            test_failed=1
        fi
    else
        print_status "warning" "Unit tests directory not found (tests/unit)"
    fi
    
    # Run integration tests
    if [ -d "tests/integration" ]; then
        print_status "info" "Running integration tests..."
        if python -m unittest discover -s tests/integration -p "*.py" -v; then
            print_status "success" "Integration tests passed"
        else
            print_status "error" "Integration tests failed"
            test_failed=1
        fi
    else
        print_status "warning" "Integration tests directory not found (tests/integration)"
    fi
    
    return $test_failed
}

# Run the package tests
run_package_tests() {
    print_status "info" "Running package tests..."
    if [ -f "run_dist.py" ]; then
        if python run_dist.py; then
            print_status "success" "All tests passed"
        else
            print_status "error" "Some tests failed"
            return 1
        fi
    else
        print_status "error" "Test runner run_dist.py not found"
        return 1
    fi
}

# Main execution flow
main() {
    # Get package name (capture only the package name, not the colored output)
    package_name=$(basename "$(pwd)")
    package_name="${package_name//-/_}" # Convert dashes to underscores
    print_status "config" "Detected package name: ${MAGENTA}${package_name}${NC}"
    
    # Check and install package
    check_and_install "$package_name"
    
    # Run automated tests
    if ! run_automated_tests; then
        exit 1
    fi
    
    # Run twine check
    if ! run_twine_check; then
        exit 1
    fi
    
    # Run package tests
    if ! run_package_tests; then
        exit 1
    fi
    
    print_status "success" "All checks and tests completed successfully"
    print_status "debug" "Package ${MAGENTA}${package_name}${NC} is ready for distribution"
}

# Execute main function
main "$@"