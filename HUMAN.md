
# Human notes (Keep this at all cost)
Change version in two places
__init__py
setup.cfg

git add setup.cfg src/aia_utilities/__init__.py
git commit -m "Bump version to 0.1.14"
git tag v0.1.14
git push && git push --tags

python3 -m pip install --upgrade build twine
python3 -m build

ls -la dist   
rm dist/*0.1.13*

the password is in shared folder
PyPI-Recovery-Codes-SurgeLove-2025-08-27T12_54_38.218571.txt

export TWINE_USERNAME="__token__"
export TWINE_PASSWORD="pypi-AgENd...your-prod-token"
python3 -m twine upload dist/*

python3 -m pip install --upgrade --force-reinstall aia-utilities