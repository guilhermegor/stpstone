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