#!/bin/bash

read -p "Enter the YAML directory path: " dir_path
read -p "Enter the YAML file name (without extension): " file_name

# ensuring the path exists
mkdir -p "$dir_path"

# create yaml file
cat <<EOF > "$dir_path/$file_name.yaml"
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
  bl_io_interpreting: False
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
EOF

echo "YAML file created at: $file_path"