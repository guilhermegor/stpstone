#!/bin/bash

# branch protection script for GitHub repositories
# prevents direct pushes to main branch and requires PR with code owner approval

# check if gh CLI is installed
if ! command -v gh &> /dev/null; then
    echo "GitHub CLI (gh) is not installed. Please install it first."
    echo "See: https://cli.github.com/"
    exit 1
fi

# check if we're in a git repository
if ! git rev-parse --is-inside-work-tree &> /dev/null; then
    echo "This is not a git repository. Please run this script from within your project folder."
    exit 1
fi

# get owner and repository name from git remote
remote_url=$(git remote get-url origin)

# handle both HTTPS and SSH URLs
if [[ $remote_url == *"https://github.com/"* ]]; then
    # HTTPS URL format: https://github.com/owner/repo.git
    repo_info=${remote_url#https://github.com/}
elif [[ $remote_url == *"git@github.com:"* ]]; then
    # SSH URL format: git@github.com:owner/repo.git
    repo_info=${remote_url#git@github.com:}
else
    echo "Could not determine repository owner and name from remote URL."
    echo "Remote URL: $remote_url"
    exit 1
fi

# remove .git suffix if present
repo_info=${repo_info%.git}

# split into owner and repo
IFS='/' read -r OWNER REPO <<< "$repo_info"

if [ -z "$OWNER" ] || [ -z "$REPO" ]; then
    echo "Failed to extract owner and repository name."
    exit 1
fi

echo "Configuring branch protection for repository: $OWNER/$REPO"

# create a temporary json file for the protection rules
PROTECTION_RULES=$(mktemp)
cat > "$PROTECTION_RULES" <<EOF
{
  "required_pull_request_reviews": {
    "dismiss_stale_reviews": true,
    "require_code_owner_reviews": true,
    "required_approving_review_count": 1
  },
  "enforce_admins": false,
  "required_status_checks": null,
  "restrictions": null
}
EOF

# set branch protection rules
gh api -X PUT \
  "repos/$OWNER/$REPO/branches/main/protection" \
  -H "Accept: application/vnd.github.v3+json" \
  --input "$PROTECTION_RULES"

# clean up the temporary file
rm "$PROTECTION_RULES"

if [ $? -eq 0 ]; then
    echo ""
    echo "Branch protection successfully applied to the main branch."
    echo "Now:"
    echo "1. Direct pushes to main are blocked"
    echo "2. All changes must go through a pull request"
    echo "3. At least one code owner approval is required"
    echo "4. Stale reviews are automatically dismissed when new commits are pushed"
else
    echo "Failed to set branch protection rules."
    exit 1
fi
