#!/bin/bash

# cli/test_dist.sh

# Get the repository/package name from the current directory
repo_name=$(basename "$(pwd)")
package_name="${repo_name//-/_}"  # Convert dashes to underscores (common in Python packages)

echo "Detected package name: $package_name"

# Check if package is installed
python -c "
import importlib.util, sys, subprocess
package_name = '$package_name'
spec = importlib.util.find_spec(package_name)
if spec is None:
    print(f'{package_name} not installed. Installing now...')
    import subprocess
    subprocess.run(['make', 'install_dist_locally'], check=True)
else:
    import pkg_resources
    try:
        current_version = pkg_resources.get_distribution(package_name).version
        print(f'{package_name} version {current_version} is already installed.')
        answer = input('Do you want to reinstall? [y/N]: ').strip().lower()
        if answer == 'y':
            print('Reinstalling...')
            subprocess.run(['make', 'install_dist_locally'], check=True)
    except pkg_resources.DistributionNotFound:
        print(f'{package_name} not properly installed. Installing now...')
        subprocess.run(['make', 'install_dist_locally'], check=True)
"

# Run the tests
if [ -f "test_package_locally.py" ]; then
    python test_package_locally.py
else
    echo "Error: test_package_locally.py not found in current directory"
    exit 1
fi