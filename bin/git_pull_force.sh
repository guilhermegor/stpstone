#!/bin/bash

git checkout main
# fetch all changes from the remote repository
git fetch --all
# delete local branches that no longer exist on github
# 1. git branch --vv: list all local branches with tracking information
# 2. grep ': gone]': filters branches marked ': gone ]' (i.e., branches that no
#   longer exist on the remote)
# 3. awk '{if ($1 == "*") print $2; else print $1}': extracts the branch name,
#   handling the case where the current branch is marked with *
# 4. xargs git branch -D: deletes the branches
git branch -vv | grep ': gone]' | awk '{if ($1 == "*") print $2; else print $1}' | xargs -r git branch -D
# update all local branches to match the remote party
#   1. git branch -r: list all remote branches
#   2. grep -v '\->': excludes the HEAD pointer
#   3. while read remote; do ... done: iterates over each remote branch
#   4. branch=${remote#origin/}: extracts the branch name (e.g., main from origin/main)
#   5. git checkout -B "$branch" "$remote": updates the local branch to match the
#      remote branch
git branch -r | grep -v '\->' | while read remote; do
	branch=${remote#origin/}
	git checkout -B "$branch" "$remote"
done
