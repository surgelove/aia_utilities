# aia_utilities
Utilities that are shared between Aia microservices.

## Building and publishing to PyPI

1. Build distributions:

```bash
python -m pip install --upgrade build twine
python -m build
```

2. (Optional) Test upload to Test PyPI:

```bash
python -m twine upload --repository testpypi dist/*
```

3. Upload to production PyPI:

```bash
python -m twine upload dist/*
```

4. Install and verify:

```bash
pip install aia-utilities
python -c "from aia_utilities import Redis_Utilities; print(Redis_Utilities)"
```

## Troubleshooting Test PyPI 403 Forbidden errors

If `twine upload` to Test PyPI returns a `403 Forbidden` with "Invalid or non-existent
authentication information", follow these steps:

1. Ensure you created an API token on Test PyPI: https://test.pypi.org/manage/account/#api-tokens
   - When creating a token, copy it immediately. You cannot view it again.

2. Use the token as your password and `__token__` as username.

Recommended (temporary) environment variables:

```bash
export TWINE_USERNAME='__token__'
export TWINE_PASSWORD='pypi-AgENd...your-token-here'
python -m twine upload --repository testpypi dist/*
```

Or store credentials in `~/.pypirc`:

```ini
[distutils]
index-servers =
    pypi
    testpypi

[testpypi]
repository: https://test.pypi.org/legacy/
username: __token__
password: pypi-AgENd...your-token-here

[pypi]
repository: https://upload.pypi.org/legacy/
username: __token__
password: pypi-AgENd...your-token-here
```

Set `~/.pypirc` permissions to be readable only by you:

```bash
chmod 600 ~/.pypirc
```

If you still get 403:

- Verify you used the Test PyPI token (tokens are separate between test and prod).
- Check for typos or truncated tokens when copying.
- Make sure the token hasn't been deleted or expired.
- Confirm you're pointing to the correct repository URL (`--repository testpypi` uses the Test PyPI endpoint).

## Basic usage example

```python
from aia_utilities import Redis_Utilities

ru = Redis_Utilities()
ru.write('my:key:1', {'timestamp': 1, 'value': 'hello'})
print(ru.read_all('my:key'))
```
# aia_utilities
Utilities that are shared between Aia microservices.


# Human notes (Keep this at all cost)
Change version in two places
__init__py
setup.cfg

git add setup.cfg src/aia_utilities/__init__.py
git commit -m "Bump version to 0.1.13"
git tag v0.1.13
git push && git push --tags

python3 -m pip install --upgrade build twine
python3 -m build

ls -la dist   
rm dist/*0.1.12*

export TWINE_USERNAME="__token__"
export TWINE_PASSWORD="pypi-AgENd...your-prod-token"
python3 -m twine upload dist/*

python3 -m pip install --upgrade --force-reinstall aia-utilities

