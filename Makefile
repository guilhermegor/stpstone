# -------------------
# VSCODE CONFIG
# -------------------
.PHONY: vscode_init export_deps

vscode_init:
	@bash bin/vscode_keybindings.sh
	@bash bin/vscode_extensions.sh

export_deps:
	@bash bin/export_deps.sh


# -------------------
# TESTING
# -------------------
.PHONY: unit_tests integration_tests test_cov test_feat test_urls_docstrings fix_playwright

unit_tests:
	@poetry run pytest tests/unit/

integration_tests:
	@poetry run pytest tests/integration/

test_cov:
	@poetry run pytest --cov=stpstone tests/unit/
	@poetry run coverage report -m
	@poetry run coverage-badge -o coverage.svg -f

test_feat:
	@bash bin/test_feature.sh $(MODULE)

test_urls_docstrings:
	@bash bin/test_urls_docstrings.sh

fix_playwright:
	@bash bin/fix_playwright.sh


# -------------------
# GIT OPERATIONS
# -------------------
.PHONY: precommit_update git_pull_force git_merge_branches git_create_branch_from_main \
        git_delete_branch_tag git_delete_dev_branches git_update_branch_with_main

precommit_update:
	@poetry run pre-commit install
	@poetry run pre-commit install --hook-type commit-msg

git_pull_force:
	@bash bin/git_pull_force.sh

git_merge_branches:
	@bash bin/git_merge_branches.sh

git_create_branch_from_main:
	@bash bin/git_create_branch_from_main.sh

git_delete_branch_tag:
	@bash bin/git_delete_branch_tag.sh

git_delete_dev_branches:
	@bash bin/git_delete_dev_branches.sh

git_update_branch_with_main:
	@bash bin/git_update_branch_with_main.sh -b $(BRANCH) $(if $(FORCE),--force)


# -------------------
# GITHUB ACTIONS
# -------------------
.PHONY: create_ssh_key gh_status gh_protect_main gh_set_pypi_secret

create_ssh_key:
	@bash bin/create_ssh_key.sh

gh_status:
	@bash bin/gh_status.sh

gh_protect_main: gh_status
	@bash bin/gh_protect_main.sh

gh_set_pypi_secret:
	@bash bin/gh_set_pypi_secret.sh
	@echo -e "\033[0;34m[i]\033[0m Checking GitHub secrets..."
	@gh secret list


# -------------------
# PACKAGE MANAGEMENT
# -------------------
.PHONY: package_tree bump_version clean_builds install_dist_locally test_dist \
        publish_test_pypi check_test_pypi check_pypi

package_tree:
	@poetry run python -c "import os; from stpstone.utils.parsers.folders import FoldersTree; \
		root_path = os.getcwd(); \
		cls_tree = FoldersTree(os.path.join(root_path, 'stpstone'), \
			bool_ignore_dot_folders=True, list_ignored_folders=['__pycache__'], \
			bool_add_linebreak_markdown=False); \
		cls_tree.export_tree(os.path.join(root_path, 'data', 'package_tree.txt'))"

bump_version:
	@bash bin/bump_version.sh

clean_builds:
	@rm -rf dist/ build/ *.egg-info/

install_dist_locally:
	@poetry build
	@poetry install
	@poetry run python -c "from stpstone.utils.parsers.folders import FoldersTree; print(\"Package import works\")"
	@poetry run python -c "import stpstone; print(stpstone.__version__)"

test_dist:
	@bash bin/test_dist.sh

publish_test_pypi: clean_builds
	@yes | bash bin/test_dist.sh
	@bash bin/test_pypi_publish.sh

check_test_pypi:
	@bash bin/docker_init.sh
	@docker build -f containers/check_test_pypi -t stpstone-test .
	@docker run -it --rm stpstone-test

check_pypi:
	@bash bin/docker_init.sh
	@read -p "Enter stpstone version to test (leave empty for latest): " version; \
	if [ -z "$$version" ]; then \
		echo "Testing latest version"; \
		docker build -f containers/check_pypi -t stpstone .; \
	else \
		echo "Testing version $$version"; \
		docker build -f containers/check_pypi --build-arg STPSTONE_VERSION=$$version -t stpstone .; \
	fi
	@docker run -it --rm stpstone


# -------------------
# INGESTION CREATORS
# -------------------
.PHONY: ingestion_concrete_creator ingestion_concrete_creator_html_parser

ingestion_concrete_creator:
	@bash bin/cc_ingestion_yaml.sh
	@bash bin/cc_ingestion_py.sh

ingestion_concrete_creator_html_parser:
	@bash bin/cc_ingestion_yaml.sh
	@bash bin/cc_ingestion_html_parser_py.sh


# -------------------
# HELP
# -------------------
.PHONY: help

help:
	@echo "Available targets:"
	@echo ""
	@echo "VSCode Configuration:"
	@echo "  vscode_init          - Initialize VSCode settings and extensions"
	@echo "  export_deps          - Export project dependencies"
	@echo ""
	@echo "Testing:"
	@echo "  unit_tests           - Run unit tests"
	@echo "  integration_tests    - Run integration tests"
	@echo "  test_cov             - Run tests with coverage report"
	@echo "  test_feat MODULE=... - Test specific feature module"
	@echo "  test_urls_docstrings - Test URL docstrings"
	@echo "  fix_playwright       - Fix Playwright installation"
	@echo ""
	@echo "Git Operations:"
	@echo "  precommit_update            - Update pre-commit hooks"
	@echo "  git_pull_force              - Force pull from remote"
	@echo "  git_merge_branches          - Merge branches"
	@echo "  git_create_branch_from_main - Create new branch from main"
	@echo "  git_delete_branch_tag       - Delete branch/tag"
	@echo "  git_delete_dev_branches     - Delete development branches"
	@echo "  git_update_branch_with_main BRANCH=... [FORCE=1] - Update branch with main"
	@echo ""
	@echo "Package Management:"
	@echo "  package_tree         - Generate package tree structure"
	@echo "  bump_version         - Bump package version"
	@echo "  clean_builds         - Clean build artifacts"
	@echo "  install_dist_locally - Install local distribution"
	@echo "  test_dist            - Test distribution"
	@echo "  publish_test_pypi    - Publish to test PyPI"
	@echo "  check_test_pypi      - Check test PyPI package"
	@echo "  check_pypi           - Check PyPI package (interactive version selection)"
	@echo ""
	@echo "Ingestion Creators:"
	@echo "  ingestion_concrete_creator          - Create ingestion concrete classes"
	@echo "  ingestion_concrete_creator_html_parser - Create HTML parser ingestion classes"