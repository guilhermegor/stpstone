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
	bash cli/dp_factory_create_req_yaml.sh
	bash cli/dp_factory_create_req_cls_py.sh

concrete_creator_ingestion_request_html_parser:
	bash cli/dp_factory_html_parser_create_req_yaml.sh
	bash cli/dp_factory_html_parser_create_req_cls_py.sh


### GIT ###

git_gh_status:
	bash cli/git_gh_status.sh

git_protect_main: git_gh_status
	bash cli/git_protect_main.sh

git_pull_force:
	bash cli/git_pull_force.sh


### REQUIREMENTS - DEV ###

vscode_install_extensions:
	bash cli/install_vscode_extensions.sh
