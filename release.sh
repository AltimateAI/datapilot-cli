#!/bin/bash

# Exit on any error
set -e

# Step 1: Remove the build directory
echo "Removing build directory..."
rm -rf build
if [ $? -ne 0 ]; then
    echo "Error: Failed to remove build directory."
    exit 1
fi

# Step 2: Remove egg-info files from the src directory
echo "Removing egg-info files from src directory..."
rm -rf src/*.egg-info
if [ $? -ne 0 ]; then
    echo "Error: Failed to remove egg-info files."
    exit 1
fi

# Step 3: Run tox checks
echo "Running tox checks..."
tox -e check
if [ $? -ne 0 ]; then
    echo "Error: tox checks failed."
    exit 1
fi

# Step 4: Clean the setup and create source and wheel distributions
echo "Cleaning setup and creating distributions..."
python setup.py clean --all sdist bdist_wheel
if [ $? -ne 0 ]; then
    echo "Error: Failed to clean setup and create distributions."
    exit 1
fi

# Step 5: Upload the distributions to PyPI, skipping any files that already exist
echo "Uploading distributions to PyPI..."
twine upload --skip-existing dist/*.whl dist/*.gz
if [ $? -ne 0 ]; then
    echo "Error: Failed to upload distributions to PyPI."
    exit 1
fi

echo "Release process completed successfully."
