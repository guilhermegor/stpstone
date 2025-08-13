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

validate_branch_name() {
    local branch_name=$1
    local pattern="^(feat|bugfix|hotfix|release|docs|refactor|chore|experiment|test|spike|perf)\/.+$"
    
    if [[ ! $branch_name =~ $pattern ]]; then
        print_status "error" "Branch name must start with one of: feat/, bugfix/, hotfix/, release/, docs/, refactor/, chore/, experiment/, test/, spike/, perf/"
        print_status "error" "Followed by a description (e.g., feat/add-new-feature)"
        return 1
    fi
    return 0
}

branch_exists_remote() {
    local branch_name=$1
    git fetch --all > /dev/null 2>&1
    if git show-ref --verify --quiet "refs/remotes/origin/$branch_name"; then
        return 0
    else
        return 1
    fi
}

branch_exists_local() {
    local branch_name=$1
    if git show-ref --verify --quiet "refs/heads/$branch_name"; then
        return 0
    else
        return 1
    fi
}

is_local_ahead() {
    local branch_name=$1
    if ! branch_exists_remote "$branch_name"; then
        return 1
    fi
    
    local local_commit=$(git rev-parse "$branch_name")
    local remote_commit=$(git rev-parse "origin/$branch_name")
    
    if [ "$local_commit" != "$remote_commit" ]; then
        git merge-base --is-ancestor "$remote_commit" "$local_commit"
        return $?
    fi
    return 1
}

validate_version_format() {
    local version=$1
    if [[ ! $version =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
        print_status "error" "Invalid version format. Must be MAJOR.MINOR.PATCH (e.g., 1.0.0)"
        return 1
    fi
    return 0
}

tag_exists() {
    local tag_name=$1
    git fetch --tags > /dev/null 2>&1
    if git show-ref --tags --verify --quiet "refs/tags/$tag_name"; then
        return 0
    else
        return 1
    fi
}

create_version_tag() {
    local branch_name=$1
    
    # Extract version from release branch name
    local version=${branch_name#release/}
    
    # Validate version format
    if ! validate_version_format "$version"; then
        return 1
    fi
    
    local tag_name="v$version"
    
    # Check if tag already exists
    if tag_exists "$tag_name"; then
        print_status "warning" "Tag $tag_name already exists"
        return 1
    fi
    
    # Create tag
    print_status "info" "Creating tag $tag_name for release $version"
    git tag -a "$tag_name" -m "Version $version"
    git push origin "$tag_name" --tags
    
    if [ $? -eq 0 ]; then
        print_status "success" "Successfully created and pushed tag $tag_name"
        return 0
    else
        print_status "error" "Failed to create tag $tag_name"
        return 1
    fi
}

create_or_checkout_branch() {
    local branch_name=$1
    
    # validate branch name format
    if ! validate_branch_name "$branch_name"; then
        exit 1
    fi
    
    # fetch all branches first
    print_status "info" "Fetching all branches from remote..."
    git fetch --all
    
    # checkout main and pull latest
    print_status "info" "Updating main branch..."
    git checkout main
    git pull origin main
    
    # check if branch exists (remote or local)
    if branch_exists_remote "$branch_name"; then
        print_status "info" "Branch exists on remote. Checking out and pulling latest..."
        
        # check if exists locally
        if branch_exists_local "$branch_name"; then
            # check if local is ahead
            if is_local_ahead "$branch_name"; then
                print_status "info" "Local branch is ahead of remote. Just checking out..."
                git checkout "$branch_name"
            else
                print_status "info" "Local branch is not ahead. Checking out and pulling..."
                git checkout "$branch_name"
                git pull origin "$branch_name"
            fi
        else
            # create local tracking branch
            git checkout -b "$branch_name" "origin/$branch_name"
        fi
    elif branch_exists_local "$branch_name"; then
        print_status "info" "Branch exists locally but not on remote. Checking out..."
        git checkout "$branch_name"
    else
        print_status "info" "Creating new branch: $branch_name"
        git checkout -b "$branch_name"
        git push -u origin "$branch_name"
    fi
    
    print_status "success" "Done! You're now on branch: $branch_name"
    
    # if this is a release branch, create a version tag
    if [[ $branch_name =~ ^release/ ]]; then
        print_status "info" "Release branch detected. Creating version tag..."
        create_version_tag "$branch_name"
    fi
}

read -p "Enter branch name (format: type/description, e.g., feat/add-new-feature): " branch_name

create_or_checkout_branch "$branch_name"