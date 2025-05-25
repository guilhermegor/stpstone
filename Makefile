# package
package_tree:
	python -c "import os; from stpstone.utils.parsers.folders import FoldersTree; \
		root_path = os.getcwd(); \
		cls_tree = FoldersTree(os.path.join(root_path, 'stpstone'), \
			bl_ignore_dot_folders=True, list_ignored_folders=['__pycache__'], \
			bl_add_linebreak_markdown=False); \
		cls_tree.export_tree(os.path.join(root_path, 'data', 'package_tree.txt'))"

install_dist_locally:
	python setup.py sdist
	pip install .

test_dist:
	bash cli/test_dist.sh


# ingestion concrete creator - factory design pattern
ingestion_concrete_creator:
	bash cli/cc_ingestion_yaml.sh
	bash cli/cc_ingestion_py.sh

ingestion_concrete_creator_html_parser:
	bash cli/cc_ingestion_yaml.sh
	bash cli/cc_ingestion_html_parser_py.sh


# git / github
git_pull_force:
	bash cli/git_pull_force.sh

git_merge_branches:
	bash cli/git_merge_branches.sh

git_create_branch_from_main:
	bash cli/git_create_branch_from_main.sh

git_delete_dev_branches:
	bash cli/git_delete_dev_branches.sh

gh_status:
	bash cli/gh_status.sh

gh_protect_main: gh_status
	bash cli/gh_protect_main.sh

gh_actions_local_tests:
	bash cli/act_install.sh
	bash cli/docker_init.sh
	bash cli/act_tests.sh


# requirements - dev
vscode_install_extensions:
	bash cli/install_vscode_extensions.sh
