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

# function to check if git repository is clean
check_git_status() {
    print_status "info" "Checking git status..."
    
    # check for unstaged changes
    if ! git diff-index --quiet HEAD --; then
        print_status "error" "There are unstaged changes in the repository. Please commit or stash them before proceeding."
        return 1
    fi
    
    # check if any branch is ahead of main
    git fetch origin main &> /dev/null
    local problematic_branches=()
    
    # get all local branches except main and release/*
    local branches_to_check=$(git branch | grep -v "main" | grep -v "release/" | sed 's/^* //' | sed 's/^ *//')
    
    for branch in $branches_to_check; do
        local commits_ahead=$(git rev-list --count main..$branch 2>/dev/null)
        if [ "$commits_ahead" -gt 0 ]; then
            problematic_branches+=("$branch (${commits_ahead} commit(s) ahead)")
        fi
    done
    
    if [ ${#problematic_branches[@]} -gt 0 ]; then
        print_status "error" "The following branches are ahead of main:"
        for branch_info in "${problematic_branches[@]}"; do
            print_status "error" "  - $branch_info"
        done
        print_status "error" "Please merge or rebase these branches before deletion."
        return 1
    fi
    
    print_status "success" "Git status check passed."
    return 0
}

# function to checkout main branch
checkout_main() {
    print_status "info" "Checking out main branch..."
    
    if git checkout main &> /dev/null; then
        print_status "success" "Successfully checked out main branch."
        return 0
    else
        print_status "error" "Failed to checkout main branch."
        return 1
    fi
}

# function to delete all branches except main and release/*
delete_branches() {
    print_status "info" "Deleting branches (keeping main and release/*)..."
    
    # fetch all branches from remote
    git fetch --prune &> /dev/null
    
    # get list of branches to delete (local and remote branches except main and release/*)
    local local_branches_to_delete=$(git branch | grep -v "main" | grep -v "release/" | sed 's/^* //' | sed 's/^ *//')
    local remote_branches_to_delete=$(git branch -r | grep -v "origin/main" | grep -v "origin/release/" | sed 's/^ *origin\///' | sed 's/^ *//')
    
    if [ -z "$local_branches_to_delete" ] && [ -z "$remote_branches_to_delete" ]; then
        print_status "info" "No branches to delete."
        return 0
    fi
    
    # delete local branches
    local local_error_occurred=0
    for branch in $local_branches_to_delete; do
        if git branch -D "$branch" &> /dev/null; then
            print_status "success" "Deleted local branch: $branch"
        else
            print_status "error" "Failed to delete local branch: $branch"
            local_error_occurred=1
        fi
    done
    
    # delete remote branches
    local remote_error_occurred=0
    for branch in $remote_branches_to_delete; do
        if git push origin --delete "$branch" &> /dev/null; then
            print_status "success" "Deleted remote branch: origin/$branch"
        else
            print_status "error" "Failed to delete remote branch: origin/$branch"
            remote_error_occurred=1
        fi
    done
    
    if [ $local_error_occurred -eq 0 ] && [ $remote_error_occurred -eq 0 ]; then
        print_status "success" "Branch cleanup completed successfully (local and remote)."
        return 0
    else
        print_status "warning" "Branch cleanup completed with some errors."
        return 1
    fi
}

# main function
main() {
    # check if we're in a git repository
    if ! git rev-parse --is-inside-work-tree &> /dev/null; then
        print_status "error" "This is not a git repository."
        exit 1
    fi
    
    # perform checks
    if ! check_git_status; then
        exit 1
    fi
    
    # checkout main
    if ! checkout_main; then
        exit 1
    fi
    
    # delete branches
    delete_branches
}

# execute main function
main