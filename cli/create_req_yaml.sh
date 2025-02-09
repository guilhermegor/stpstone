#!/bin/bash

# define the project root directory
PROJECT_ROOT="$(pwd)/stpstone"

# prompt for folder path within the project
read -p "Enter the folder path within the project (default: ./_config): " folder_path
folder_path=${folder_path:-./_config}

# ensure the folder path is within the project directory
if [[ "$folder_path" != ./* ]]; then
  echo "Error: The folder path must be within the project directory."
  exit 1
fi

# construct the full directory path
full_dir_path="$PROJECT_ROOT/$folder_path"

# ensure the directory exists
mkdir -p "$full_dir_path"

# prompt for YAML file name
read -p "Enter the YAML file name (without extension, default: request_config): " file_name
file_name=${file_name:-request_config}

# create yaml file
cat <<EOF > "$full_dir_path/$file_name.yaml"
### CONFIGURATION TOOLS###

credentials:
  host:
  headers:
  payload:
  token:
    host:
    get:
      req_method: GET
      bl_verify: False
      timeout: (12.0, 12.0)
    keys:
      token: token

logs:
  dtypes:
    FILE_NAME: str
    REF_DATE: date

metadata:

downstream_processes:


### RESOURCES TO BE SCRAPED ###

resource_example:
  req_method: GET
  # if host is not defined within credentials, use the source one
  host:
  headers:
  payload:
  # for iteratively_get_data, please use i as placeholder, as https://example.com/{i}
  app:
  bl_verify: False
  timeout: (12.0, 12.0)
  cols_from_case:
  cols_to_case:
  str_fmt_dt: YYYY-MM-DD
  type_error_action: raise
  strt_keeping_when_duplicated: first
  schema: RAW
  table_name:
  bl_insert_or_ignore: False
  bl_schema: True
  dtypes:
    COL_1: date
    COL_2: str
    COL_3: int
    COL_4: float
    COL_5: category
  xml_keys:
    tags:
      tag_parent:
        - col1
        - col2
        - col3
        - col4
        - col5
    attributes:
      attrb: Attrb
EOF

echo "YAML file created at: $file_path"