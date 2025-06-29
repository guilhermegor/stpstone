# package
package_tree:
	python -c "import os; from stpstone.utils.parsers.folders import FoldersTree; \
		root_path = os.getcwd(); \
		cls_tree = FoldersTree(os.path.join(root_path, 'stpstone'), \
			bl_ignore_dot_folders=True, list_ignored_folders=['__pycache__'], \
			bl_add_linebreak_markdown=False); \
		cls_tree.export_tree(os.path.join(root_path, 'data', 'package_tree.txt'))"

bump_version:
	bash cli/bump_version.sh

clean_builds:
	rm -rf dist/ build/ *.egg-info/

install_dist_locally:
	poetry build
	poetry install
	poetry run python -c "from stpstone.utils.parsers.folders import FoldersTree; print(\"Package import works\")"
	poetry run python -c "import stpstone; print(stpstone.__version__)"

test_dist:
	bash cli/test_dist.sh

publish_test_pypi: clean_builds
	yes | bash cli/test_dist.sh
	bash cli/test_pypi_publish.sh

check_test_pypi:
	bash cli/docker_init.sh
	docker build -f containers/check_test_pypi -t stpstone-test .
	docker run -it --rm stpstone-test

check_pypi:
	@bash cli/docker_init.sh
	@read -p "Enter stpstone version to test (leave empty for latest): " version; \
	if [ -z "$$version" ]; then \
		echo "Testing latest version"; \
		docker build -f containers/check_pypi -t stpstone .; \
	else \
		echo "Testing version $$version"; \
		docker build -f containers/check_pypi --build-arg STPSTONE_VERSION=$$version -t stpstone .; \
	fi
	@docker run -it --rm stpstone


# ingestion concrete creator - factory design pattern
ingestion_concrete_creator:
	bash cli/cc_ingestion_yaml.sh
	bash cli/cc_ingestion_py.sh

ingestion_concrete_creator_html_parser:
	bash cli/cc_ingestion_yaml.sh
	bash cli/cc_ingestion_html_parser_py.sh


# git
precommit_update:
	poetry run pre-commit install
	poetry run pre-commit install --hook-type commit-msg

git_pull_force:
	bash cli/git_pull_force.sh

git_merge_branches:
	bash cli/git_merge_branches.sh

git_create_branch_from_main:
	bash cli/git_create_branch_from_main.sh

git_delete_branch_tag:
	bash cli/git_delete_branch_tag.sh

git_delete_dev_branches:
	bash cli/git_delete_dev_branches.sh

git_update_branch_with_main:
	bash cli/git_update_branch_with_main.sh -b $(BRANCH) $(if $(FORCE),--force)


# github
create_ssh_key:
	bash cli/create_ssh_key.sh

gh_status:
	bash cli/gh_status.sh

gh_protect_main: gh_status
	bash cli/gh_protect_main.sh

gh_set_pypi_secret:
	bash cli/gh_set_pypi_secret.sh
	@echo -e "\033[0;34m[i]\033[0m Checking GitHub secrets..."
	@gh secret list


# requirements - dev
vscode_setup:
	bash cli/vscode_setup.sh

export_deps:
	bash cli/export_deps.sh
