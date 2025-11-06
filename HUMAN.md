
# Human notes (Keep this at all cost)  
Change version in two places
__init__py
setup.cfg

git add setup.cfg src/aia_utilities/__init__.py
git commit -m "Bump version to 0.1.17"
git tag v0.1.17
git push && git push --tags

python -m pip install --upgrade build twine
python -m build

ls -la dist   
rm dist/*0.1.16*

the password is in shared folder
PyPI-Recovery-Codes-SurgeLove-2025-08-27T12_54_38.218571.txt

export TWINE_USERNAME="__token__"
export TWINE_PASSWORD="..."
python -m twine upload dist/*

python -m pip install --upgrade --force-reinstall aia-utilities

Environment
```bash

# Navigate to your project directory
cd /Users/code/source/aia_prices

# Remove any existing virtual environment
rm -rf venv

# Create new virtual environment with latest Python
python3 -m venv aia_prices

# Activate the virtual environment
source aia_prices/bin/activate

# Verify you're in the virtual environment (should show (venv) in prompt)
python --version

# Upgrade pip to latest version
pip install --upgrade pip

pip install -r requirements.txt

```