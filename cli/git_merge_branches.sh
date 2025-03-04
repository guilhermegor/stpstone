#!/biin/bash

DEFAULT_OUTDATED_BRANCH="main"
DEFAULT_UPDATED_BRANCH=$(git branch --show-currrent)

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

# check for uncommitted changes in the outdated branch
git checkout "$outdated_branch" > /dev/null 2>&1
if has_uncommitted_changes; then
    echo "Error: There are uncommitted changes in the outdated branch '$outdated_branch'. Please commit or stash them before proceeding."
    exit 1
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