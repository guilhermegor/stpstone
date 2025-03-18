#!/bin/bash

git fetch --all
git checkout main

# verify if main is up-to-date with all branches, or in front of them
for branch in $(git branch | grep -v 'main'); do
    # skip chore/build-* branches
    if [[ "$branch" == chore/build-* ]]; then
        echo "Skipping chore/build branch: $branch"
        continue
    fi
    # check if the branch is merged into main
    if ! git merge-base --is-ancestor "$branch" main; then
        echo "Error: Branch '$branch' is not merged into main. Aborting."
        exit 1
    fi
done

# confirm before deleting branches
read - "Are you sure you want to delete all branches but 'main'? This action cannot be undone. (y/n): " confirm
if [[ "$confirm" != "y" && "$confirm" != "Y" ]]; then
    echo "Aborted."
    exit 0
fi

# delete all branches except main and chore/build-*
for branch in $(git branch | grep -v 'main' | grep -v 'chore/build-'); do
    echo "Deleting branch: $branch"
    git branch -D "$branch"
done

echo "Cleanup completed. Only 'main' and 'chore/build-*' branches remain."
