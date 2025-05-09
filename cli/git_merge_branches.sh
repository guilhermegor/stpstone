#!/bin/bash

DEFAULT_UPDATED_BRANCH="main"
DEFAULT_OUTDATED_BRANCH=$(git branch --show-current)

branch_exists() {
    git show-ref --verify --quiet refs/heads/"$1"
}
has_uncommited_changes() {
    ! git diff-index --quiet HEAD --
}
prompt_branch_name() {
    local prompt_message="$1"
    local default_branch="$2"
    read -p "$prompt_message (default: $default_branch): " branch
    echo "${branch:-$default_branch}"
}

# check if we're in a git repository
if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    echo "Error: This is not a git repository."
    exit 1
fi

# prompts for requesting user input
outdated_branch=$(prompt_branch_name "Enter the name of the outdated branch" "$DEFAULT_OUTDATED_BRANCH")
updated_branch=$(prompt_branch_name "Enter the name of the updated branch" "$DEFAULT_UPDATED_BRANCH")

# check if branches exist
branches=("$updated_branch" "$outdated_branch")
for branch in "${branches[@]}"; do
    if ! branch_exists "$branch"; then
        echo "Error: The branch '$branch' does not exist."
        exit 1
    fi
done

# check for uncommitted changes
for branch in "${branches[@]}"; do
    git checkout "$branch" > /dev/null 2>&1
    if has_uncommited_changes; then
        echo "Error: There are uncommitted changes in the branch '$branch'. Please commit or stash them before proceeding."
        exit 1
    fi
done

# fetch all remote branches
git fetch --all

# check if origin/updated_branch exists
if ! git show-ref --verify --quiet refs/remotes/origin/"$updated_branch"; then
    echo "Warning: The remote branch 'origin/$updated_branch' does not exist. Continuing with local branch only."
else
    # compare local updated branch with origin/updated_branch
    LOCAL=$(git rev-parse "$updated_branch")
    REMOTE=$(git rev-parse "origin/$updated_branch")
    BASE=$(git merge-base "$updated_branch" "origin/$updated_branch")

    if [ "$LOCAL" != "$REMOTE" ] && [ "$LOCAL" = "$BASE" ]; then
        echo "Error: Your local '$updated_branch' branch is behind 'origin/$updated_branch'. Please pull the latest changes first."
        exit 1
    fi
fi

# confirm before merging
read -p "Are you sure you want to merge '$updated_branch' into '$outdated_branch'? This will overwrite any changes in '$outdated_branch'. (y/n): " confirm

if [[ "$confirm" != "y" ]]; then
    echo "Merge aborted."
    exit 0
fi

# perform the merge
git fetch --all
git checkout "$outdated_branch"
git merge -Xtheirs "$updated_branch"

# push the changes
git push origin "$outdated_branch"

echo "Merge completed successfully."