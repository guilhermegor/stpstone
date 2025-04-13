### BUILDING PACKAGE ###

clean_dist:
	cd $(PROJECT_ROOT) && rm -rf dist

build_package_pypi_org: clean_dist
	python -m build
	echo "Files to be uploaded: dist/*"
	echo "Building and uploading to pypi.org..."
	export $$(grep -v '^#' .env | xargs) && \
		python -m twine upload -u "$$PYPI_USERNAME" -p "$$PYPI_PASSWORD" dist/*

build_package_test_pypi_org: clean_dist
	python -m build
	echo "Files to be uploaded: dist/*"
	echo "Building and uploading to test.pypi.org..."
	export $$(grep -v '^#' .env | xargs) && \
		python -m twine upload -u "$$PYPI_USERNAME" -p "$$PYPI_PASSWORD" \
		--repository-url https://test.pypi.org/legacy/ dist/*


### FACTORY METHODS ###

concrete_creator_ingestion_request:
	bash cli/create_req_yaml.sh
	bash cli/create_req_cls_py.sh

concrete_creator_ingestion_request_html_parser:
	bash cli/html_parser_create_req_yaml.sh
	bash cli/html_parser_create_req_cls_py.sh


### GIT ###

git_pull_force:
	bash cli/git_pull_force.sh

git_keep_just_main_builds:
	bash cli/git_keep_just_main.sh

git_merge_branches:
	bash cli/git_merge_branches.sh


### REQUIREMENTS - DEV ###

vscode_install_extensions:
	bash cli/install_vscode_extensions.sh
