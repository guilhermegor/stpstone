#!/bin/bash

# define the project root directory
PROJECT_ROOT="$(pwd)/stpstone"

# prompt for folder path within the project
read -p "Enter the YAML folder path within the project (default: ./_config): " folder_path
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
### CONFIGURATION TOOLS ###

credentials:
  host:
  headers:
  payload:
  token:
    host:
    app:
    get:
      req_method: GET
      bl_verify: False
      timeout: (12.0, 12.0)
    keys:
      token: token

metadata:


### RESOURCES TO BE SCRAPED ###

resource_example:
  req_method: GET
  # if host is not defined within credentials, use the source one
  host:
  headers:
  payload:
  # list of slugs (complementary descriptive text to access a web pages)
  slugs:
  # use this argument only if {{chunk_slugs}} is passed to app
  int_chunk_slugs:
  # iteratively_get_data placeholders:
  #   - i (int): https://example.com/{{i}}
  #   - slug (str): https://example.com/{{slug}} - slugs in list format
  #   - chunk_slugs (List[str]): https://example.com/{{chunk_slugs}}
  # non-iteratively_get_data placeholders:
  #   - {{replacer}}: https://example.com/{{replacer}}
  #     note: the replacer can be any variable referenced within concrete product class
  # expected comments to app, within url:
  #   - feat=read_tables
  #   - int_pgs_join={{number}}
  #   - file_extension=.{{file_extension}}
  #   - start with a commentary hash and separate params with &
  app:
  bl_verify: False
  timeout: (12.0, 12.0)
  cols_from_case: default
  cols_to_case: upper_constant
  list_cols_drop_dupl:
  str_fmt_dt: YYYY-MM-DD
  type_error_action: raise
  strategy_keep_when_dupl: first
  list_ignorable_exceptions:
  schema: raw
  table_name:
  bl_insert_or_ignore: False
  bl_schema: True
  ignored_file_extensions:
    - tmp
    - log
    - bak
  dtypes:
    COL_1: date
    COL_2: str
    COL_3: int
    COL_4: float
    COL_5: category
  df_read_params:
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
  regex_patterns:
    event_1:
      condition_1: pattern_regex_1
      condition_2: pattern_regex_2
    event_2:
      condition_1: pattern_regex_1
      condition_2: pattern_regex_2
  xpaths:
    name_1: xpath_1
    name_2: xpath_2
  fixed_width_layout:
    - field: NAME_FIELD_1
      start: 0
      end: 6
    - field: NAME_FIELD_1
      start: 6
      end: 9
EOF

echo "File succesfully created at: $full_dir_path/$file_name.yaml"
