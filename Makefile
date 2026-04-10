# -------------------
# DEV ENVIRONMENT
# -------------------
.PHONY: venv update_venv precommit claude_login

init: venv precommit

venv:
	@pyenv install 3.9.22 -s
	@pyenv local 3.9.22
	@python -m pip install --upgrade pip
	@python -m pip install -r requirements.txt
	@poetry config virtualenvs.in-project true --local
	@poetry install
	@echo "Virtual environment created in ./.venv"
	@echo "Poetry project installed"
	@poetry run playwright install
	@echo "Playwright installed"

update_venv:
	@poetry update
	@echo "Poetry project updated"

precommit:
	@poetry run pre-commit install
	@poetry run pre-commit install --hook-type commit-msg

claude_login:
	@bash bin/claude_login.sh


# -------------------
# TESTING
# -------------------
.PHONY: unit_tests integration_tests test_cov test_slowest test_feat test_urls_docstrings fix_playwright

unit_tests:
	@poetry run pytest tests/unit/

integration_tests:
	@poetry run pytest tests/integration/

test_cov:
	@poetry run pytest tests/unit/ --cov=stpstone
	@poetry run coverage report -m
	@poetry run coverage-badge -o coverage.svg -f

test_slowest:
	@echo "Running tests to identify the 10 slowest tests..."
	@poetry run pytest tests/unit/ --durations=10 --tb=short

test_feat:
	@bash bin/test_feature.sh $(MODULE)

test_urls_docstrings:
	@bash bin/test_urls_docstrings.sh

fix_playwright:
	@bash bin/fix_playwright.sh


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
.PHONY: concrete_creator_ingestion

concrete_creator_ingestion:
	@bash bin/concrete_creator_ingestion.sh

# -------------------
# HELP
# -------------------
.PHONY: help

help:
	@echo "Available targets:"
	@echo ""
	@echo "Dev Environment:"
	@echo "  init                 - Set up virtualenv and install pre-commit hooks"
	@echo "  claude_login         - Authenticate with Claude Code using .env credentials"
	@echo "  venv                 - Install virtual environment dependencies"
	@echo "  update_venv          - Update virtual environment dependencies"
	@echo "  precommit            - Install pre-commit hooks"
	@echo "  python_path          - Export Python path"
	@echo "  run_script           - Run Python script in virtual environment"
	@echo ""
	@echo "Testing:"
	@echo "  unit_tests           - Run unit tests"
	@echo "  integration_tests    - Run integration tests"
	@echo "  test_cov             - Run tests with coverage report"
	@echo "  test_feat MODULE=... - Test specific feature module"
	@echo "  test_urls_docstrings - Test URL docstrings"
	@echo "  fix_playwright       - Fix Playwright installation"
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
	@echo "  concrete_creator_ingestion - Create concrete creator ingestion files"
