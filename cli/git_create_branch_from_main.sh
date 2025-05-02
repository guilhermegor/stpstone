#!/bin/bash

validate_branch_name() {
    local branch_name=$1
    local pattern="^(feat|bugfix|hotfix|release|docs|refactor|chore|experiment|test|spike|perf)\/.+$"
    
    if [[ ! $branch_name =~ $pattern ]]; then
        echo "Error: Branch name must start with one of: feat/, bugfix/, hotfix/, release/, docs/, refactor/, chore/, experiment/, test/, spike/, perf/"
        echo "Followed by a description (e.g., feat/add-new-feature)"
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

create_or_checkout_branch() {
    local branch_name=$1
    
    # validate branch name format
    if ! validate_branch_name "$branch_name"; then
        exit 1
    fi
    
    # fetch all branches first
    echo "Fetching all branches from remote..."
    git fetch --all
    
    # checkout main and pull latest
    echo "Updating main branch..."
    git checkout main
    git pull origin main
    
    # check if branch exists (remote or local)
    if branch_exists_remote "$branch_name"; then
        echo "Branch exists on remote. Checking out and pulling latest..."
        
        # check if exists locally
        if branch_exists_local "$branch_name"; then
            # check if local is ahead
            if is_local_ahead "$branch_name"; then
                echo "Local branch is ahead of remote. Just checking out..."
                git checkout "$branch_name"
            else
                echo "Local branch is not ahead. Checking out and pulling..."
                git checkout "$branch_name"
                git pull origin "$branch_name"
            fi
        else
            # create local tracking branch
            git checkout -b "$branch_name" "origin/$branch_name"
        fi
    elif branch_exists_local "$branch_name"; then
        echo "Branch exists locally but not on remote. Checking out..."
        git checkout "$branch_name"
    else
        echo "Creating new branch: $branch_name"
        git checkout -b "$branch_name"
        git push -u origin "$branch_name"
    fi
    
    echo "Done! You're now on branch: $branch_name"
}

read -p "Enter branch name (format: type/description, e.g., feat/add-new-feature): " branch_name

create_or_checkout_branch "$branch_name"