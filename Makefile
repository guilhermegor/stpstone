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


### README SUPPORT ###

package_tree:
	python -c "import os; from stpstone.utils.parsers.folders import FoldersTree; \
		root_path = os.getcwd(); \
		cls_tree = FoldersTree(os.path.join(root_path, 'stpstone'), \
			bl_ignore_dot_folders=True, list_ignored_folders=['__pycache__'], \
			bl_add_linebreak_markdown=False); \
		cls_tree.export_tree(os.path.join(root_path, 'data', 'package_tree.txt'))"

update_tree:
	@echo "Updating README.md with the latest package tree..."
	@awk 'BEGIN {FS="\n"; OFS="\n"; section_found=0} \
		/^## Project Structure/ { \
			section_found=1; \
			print; \
			print "```"; \
			system("cat data/package_tree.txt"); \
			print "```"; \
			while ((getline line > 0) && (line !~ /^```/)) {} \
		} \
		!section_found {print} \
		section_found && /^```/ {section_found=0}' README.md > README.md.tmp
	@mv README.md.tmp README.md
	@echo "README.md updated successfully."

update_readme: package_tree update_tree


### FACTORY METHODS ###

concrete_product_ingestion_request:
	bash cli/create_req_yaml.sh
	bash cli/create_req_cls_py.sh

concrete_product_ingestion_request_html_parser:
	bash cli/html_parser_create_req_yaml.sh
	bash cli/html_parser_create_req_cls_py.sh


### GIT ###

git_pull_force:
	bash cli/git_pull_force.sh

git_keep_just_main_builds:
	bash cli/git_keep_just_main.sh

git_merge_branches:
	bash cli/git_merge_branches.sh