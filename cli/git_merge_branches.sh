#!/biin/bash

DEFAULT_OUTDATED_BRANCH="main"
DEFAULT_UPDATED_BRANCH=$(git branch --show-current)

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

# check for uncommitted changes
for branch in "${branches[@]}"; do
    git checkout "$branch" > /dev/null 2>&1
    if has_uncommited_changes; then
        echo "Error: There are uncommitted changes in the branch '$branch'. Please commit them before proceeding."
        exit 1
    fi
done

# confirm before merging
read -p "Are you sure you want to merge '$updated_branch' into '$outdated_branch'? This will overwrite any changes in '$outdated_branch'. (y/n): " confirm
if [[ "$confirm" != "y" ]]; then
    echo "Merge aborted."
    git checkout "$updated_branch" > /dev/null 2>&1
    exit 0
fi

# run unittests if outdated branch is main
if [[ "$outdated_branch" == "main" ]]; then
    echo "Running unittests before merging to main branch..."
    if ! command -v python &> /dev/null; then
        echo "Error: Python is not installed or not in PATH."
        git checkout "$updated_branch" > /dev/null 2>&1
        exit 1
    fi
    if [ ! -d "tests/unit" ]; then
        echo "Error: tests/unit directory not found."
        git checkout "$updated_branch" > /dev/null 2>&1
        exit 1
    fi
    echo "Running unit tests..."
    if ! python -m unittest discover -s tests/unit -p "*.py"; then
        echo "Error: Some unittests failed. Merge aborted."
        git checkout "$updated_branch" > /dev/null 2>&1
        exit 1
    else
        echo "All unittests passed successfully."
    fi
fi

# perform the merge
git fetch --all
git checkout "$outdated_branch"
git merge -Xtheirs "$updated_branch"

# push the changes
git push origin "$outdated_branch"

echo "Merge completed successfully."
