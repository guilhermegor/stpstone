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

validate_branch() {
    local branch=$1
    if [[ "$branch" == "main" || "$branch" == "master" ]]; then
        print_status "error" "Cannot delete the $branch branch - this is a protected branch"
        exit 1
    fi
}

get_user_input() {
    local prompt="$1"
    local default="$2"
    local input
    
    if [ -n "$default" ]; then
        read -p "$prompt [$default]: " input
        input=${input:-$default}
    else
        read -p "$prompt: " input
    fi
    
    echo "$input"
}

switch_to_main() {
    print_status "info" "Switching to main branch..."
    if git checkout main; then
        print_status "success" "Successfully switched to main branch"
        git pull origin main
        return 0
    else
        print_status "error" "Failed to switch to main branch"
        return 1
    fi
}

delete_local_tag() {
    local tag=$1
    print_status "info" "Deleting local tag: $tag..."
    if git tag -d "$tag"; then
        print_status "success" "Successfully deleted local tag: $tag"
        return 0
    else
        print_status "warning" "Local tag $tag does not exist"
        return 1
    fi
}

delete_remote_tag() {
    local tag=$1
    print_status "info" "Deleting remote tag: $tag..."
    if git push origin --delete "$tag"; then
        print_status "success" "Successfully deleted remote tag: $tag"
        return 0
    else
        print_status "warning" "Failed to delete remote tag $tag (may not exist)"
        return 1
    fi
}

delete_local_branch() {
    local branch=$1
    validate_branch "$branch"
    print_status "info" "Deleting local branch: $branch..."
    if git branch -D "$branch"; then
        print_status "success" "Successfully deleted local branch: $branch"
        return 0
    else
        print_status "warning" "Local branch $branch does not exist"
        return 1
    fi
}

delete_remote_branch() {
    local branch=$1
    validate_branch "$branch"
    print_status "info" "Deleting remote branch: $branch..."
    if git push origin --delete "$branch"; then
        print_status "success" "Successfully deleted remote branch: $branch"
        return 0
    else
        print_status "warning" "Failed to delete remote branch $branch (may not exist)"
        return 1
    fi
}

clean_references() {
    print_status "info" "Cleaning up local references..."
    git fetch --prune --tags
    print_status "success" "Local references cleaned"
}

verify_deletion() {
    local tag=$1
    local branch=$2
    
    print_status "info" "Verifying deletions..."
    
    # Check tag
    if git tag -l | grep -q "$tag"; then
        print_status "error" "Tag $tag still exists!"
    else
        print_status "success" "Tag $tag successfully removed"
    fi
    
    if [[ "$branch" != "main" && "$branch" != "master" ]]; then
        if git branch -a | grep -q "$branch"; then
            print_status "error" "Branch $branch still exists!"
        else
            print_status "success" "Branch $branch successfully removed"
        fi
    fi
}

main() {
    local branch=$(get_user_input "Enter the branch name to delete" "")
    local tag=$(get_user_input "Enter the tag to delete (include 'v' prefix if needed)" "")
    
    if [[ -z "$branch" && -z "$tag" ]]; then
        print_status "error" "No branch or tag specified for deletion"
        exit 1
    fi
    
    if ! switch_to_main; then
        print_status "error" "Aborting script due to failure to switch to main"
        exit 1
    fi
    
    if [[ -n "$tag" ]]; then
        delete_local_tag "$tag"
        delete_remote_tag "$tag"
    fi
    
    if [[ -n "$branch" ]]; then
        delete_local_branch "$branch"
        delete_remote_branch "$branch"
    fi
    
    clean_references
    
    if [[ -n "$tag" || -n "$branch" ]]; then
        verify_deletion "$tag" "$branch"
    fi
    
    print_status "success" "Cleanup completed"
}

main "$@"