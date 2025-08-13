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
        "success") echo -e "${GREEN}[✓]${NC} ${message}" ;;
        "error") echo -e "${RED}[✗]${NC} ${message}" >&2 ;;
        "warning") echo -e "${YELLOW}[!]${NC} ${message}" ;;
        "info") echo -e "${BLUE}[i]${NC} ${message}" ;;
        "config") echo -e "${CYAN}[→]${NC} ${message}" ;;
        "debug") echo -e "${MAGENTA}[»]${NC} ${message}" ;;
        *) echo -e "[ ] ${message}" ;;
    esac
}

check_git_status() {
    print_status "info" "Checking git status..."

    if ! git diff-index --quiet HEAD --; then
        print_status "error" "There are unstaged changes in the repository. Please commit or stash them before proceeding."
        return 1
    fi

    git fetch origin main &> /dev/null
    local problematic_branches=()
    local branches_to_check=$(git branch | grep -v "main" | grep -v "release/" | sed 's/^* //' | sed 's/^ *//' | grep -v "^temp-branch$")

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

delete_branches() {
    print_status "info" "Deleting branches (keeping main and release/*)..."

    git fetch --prune &> /dev/null

    local local_branches_to_delete=$(git branch | grep -v "main" | grep -v "release/" | sed 's/^* //' | sed 's/^ *//' | grep -v "^temp-branch$")
    local remote_branches_to_delete=$(git branch -r | grep -v "origin/main" | grep -v "origin/release/" | sed 's/^ *origin\///' | sed 's/^ *//' | grep -v "^temp-branch$")

    local local_error_occurred=0
    for branch in $local_branches_to_delete; do
        if git branch -D "$branch" &> /dev/null; then
            print_status "success" "Deleted local branch: $branch"
        else
            print_status "error" "Failed to delete local branch: $branch"
            local_error_occurred=1
        fi
    done

    for branch in $remote_branches_to_delete; do
        if git push origin --delete "$branch" &> /dev/null; then
            print_status "success" "Deleted remote branch: origin/$branch"
        else
            print_status "error" "Failed to delete remote branch: origin/$branch"
            local_error_occurred=1
        fi
    done

    # Always delete temp-branch if it exists (local and remote)
    if git show-ref --verify --quiet refs/heads/temp-branch; then
        if git branch -D temp-branch &> /dev/null; then
            print_status "success" "Force-deleted local temp-branch"
        else
            print_status "error" "Failed to delete local temp-branch"
            local_error_occurred=1
        fi
    fi

    if git ls-remote --exit-code --heads origin temp-branch &> /dev/null; then
        if git push origin --delete temp-branch &> /dev/null; then
            print_status "success" "Deleted remote temp-branch"
        else
            print_status "error" "Failed to delete remote temp-branch"
            local_error_occurred=1
        fi
    fi

    if [ $local_error_occurred -eq 0 ]; then
        print_status "success" "Branch cleanup completed successfully."
        return 0
    else
        print_status "warning" "Branch cleanup completed with some errors."
        return 1
    fi
}

main() {
    if ! git rev-parse --is-inside-work-tree &> /dev/null; then
        print_status "error" "This is not a git repository."
        exit 1
    fi

    if ! check_git_status; then
        exit 1
    fi

    if ! checkout_main; then
        exit 1
    fi

    delete_branches
}

main