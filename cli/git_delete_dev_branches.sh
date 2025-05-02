#!/bin/bash

set -e  # Exit on error

DEFAULT_BRANCH="main"
PROTECTED_BRANCHES=("$DEFAULT_BRANCH" "release/*")

has_uncommitted_changes() {
    ! git diff-index --quiet HEAD --
}

is_ahead_of_protected() {
    local branch="$1"
    for protected in "${PROTECTED_BRANCHES[@]}"; do
        if git merge-base --is-ancestor "$protected" "$branch"; then
            false && return
        else
            echo "Error: Branch '$branch' is ahead of or diverged from '$protected'"
            true && return
        fi
    done
    false
}

if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    echo "Error: Not in a git repository"
    exit 1
fi

if has_uncommitted_changes; then
    echo "Error: You have uncommitted changes. Commit or stash them first."
    exit 1
fi

git checkout "$DEFAULT_BRANCH" >/dev/null 2>&1

# get all local branches except protected ones
LOCAL_BRANCHES_TO_DELETE=$(git branch | grep -vE "$(IFS='|'; echo "${PROTECTED_BRANCHES[*]}")" | sed 's/^* //' | tr -d ' ')

# get all remote branches except protected ones
REMOTE_BRANCHES_TO_DELETE=$(git branch -r | grep -vE "$(IFS='|'; echo "${PROTECTED_BRANCHES[*]/#/origin\/}")" | grep -v "HEAD" | sed 's/origin\///' | tr -d ' ')

# check if any branches are ahead of protected branches
ERROR_FOUND=0
for branch in $LOCAL_BRANCHES_TO_DELETE; do
    if is_ahead_of_protected "$branch"; then
        ERROR_FOUND=1
    fi
done

if [ "$ERROR_FOUND" -eq 1 ]; then
    echo "Aborting: Some branches are ahead of protected branches."
    exit 1
fi

# show what will be deleted
echo "The following LOCAL branches will be deleted:"
echo "$LOCAL_BRANCHES_TO_DELETE" | sed 's/^/ - /'
echo ""
echo "The following REMOTE branches will be deleted:"
echo "$REMOTE_BRANCHES_TO_DELETE" | sed 's/^/ - /'

read -p "Are you sure you want to delete these branches? (y/n) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Aborted."
    exit 0
fi

echo "$LOCAL_BRANCHES_TO_DELETE" | xargs -r -n 1 git branch -D

echo "$REMOTE_BRANCHES_TO_DELETE" | xargs -r -n 1 git push origin --delete

echo "Cleanup complete. Only protected branches remain."