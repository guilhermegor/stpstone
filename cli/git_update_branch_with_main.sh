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

# function to align a branch with main
align_branch_with_main() {
    local branch_name="$1"
    local force="${2:-false}"
    local stash_created=false

    print_status "info" "Starting alignment of branch ${CYAN}${branch_name}${NC} with ${BLUE}main${NC}"

    # verify we're in a git repository
    if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
        print_status "error" "Not in a Git repository"
        return 1
    fi

    # check if branch exists
    if ! git show-ref --verify --quiet refs/heads/"$branch_name"; then
        print_status "error" "Branch ${CYAN}${branch_name}${NC} does not exist locally"
        return 1
    fi

    # fetch latest changes from origin
    print_status "config" "Fetching latest changes from origin..."
    git fetch origin

    # check for uncommitted changes
    if ! git diff-index --quiet HEAD --; then
        print_status "warning" "Uncommitted changes detected - stashing..."
        if git stash push -m "auto-stash before aligning ${branch_name} with main" --include-untracked; then
            stash_created=true
            print_status "success" "Changes stashed successfully"
        else
            print_status "error" "Failed to stash changes"
            return 1
        fi
    fi

    # checkout the branch
    print_status "config" "Checking out branch ${CYAN}${branch_name}${NC}..."
    if ! git checkout "$branch_name"; then
        print_status "error" "Failed to checkout branch ${CYAN}${branch_name}${NC}"
        [ "$stash_created" = true ] && git stash pop
        return 1
    fi

    # reset to main
    print_status "config" "Resetting ${CYAN}${branch_name}${NC} to ${BLUE}main${NC}..."
    if ! git reset --hard main; then
        print_status "error" "Failed to reset branch"
        [ "$stash_created" = true ] && git stash pop
        return 1
    fi

    # push with force if requested
    if [ "$force" = "true" ]; then
        print_status "warning" "Force pushing ${CYAN}${branch_name}${NC} to origin..."
        if ! git push origin "$branch_name" --force; then
            print_status "error" "Failed to force push branch"
            [ "$stash_created" = true ] && git stash pop
            return 1
        fi
        print_status "success" "Successfully force pushed ${CYAN}${branch_name}${NC} to origin"
    else
        print_status "info" "Branch aligned locally. Use ${YELLOW}--force${NC} to push to remote."
    fi

    # restore stash if we created one
    if [ "$stash_created" = true ]; then
        print_status "config" "Restoring stashed changes..."
        if git stash pop; then
            print_status "success" "Stashed changes restored"
        else
            print_status "warning" "Failed to restore stashed changes"
        fi
    fi

    print_status "success" "Branch ${CYAN}${branch_name}${NC} successfully aligned with ${BLUE}main${NC}"
    return 0
}

# main function to handle command line arguments
main() {
    local branch_name=""
    local force=false

    # parse arguments
    while [[ $# -gt 0 ]]; do
        case "$1" in
            -b|--branch)
                branch_name="$2"
                shift 2
                ;;
            -f|--force)
                force=true
                shift
                ;;
            *)
                print_status "error" "Unknown argument: $1"
                exit 1
                ;;
        esac
    done

    # default to current branch if none specified
    if [ -z "$branch_name" ]; then
        branch_name=$(git rev-parse --abbrev-ref HEAD 2>/dev/null)
        if [ -z "$branch_name" ]; then
            print_status "error" "Could not determine current Git branch"
            exit 1
        fi
        print_status "info" "No branch specified. Using current branch: ${CYAN}${branch_name}${NC}"
    fi

    # execute alignment
    align_branch_with_main "$branch_name" "$force"
}

# execute main function with arguments
main "$@"