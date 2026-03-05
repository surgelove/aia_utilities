# aia_utilities

A Python package providing optimized Redis Streams utilities and time management functions for microservices.

## Installation

```bash
pip install aia-utilities
```

Or install from source:

```bash
pip install -e .
```

## Quick Start

```python
from aia_utilities import RedisUtilities

# Initialize with defaults (localhost:6379)
ru = RedisUtilities()

# Write data to a stream
ru.write('prices', {'symbol': 'AAPL', 'price': 150.25, 'timestamp': '2026-01-01 10:00:00'}, maxlen=10000)

# Read all entries
entries = ru.read_all('prices')

# Get the latest entry matching a field
latest = ru.get_latest('prices', 'symbol', 'AAPL')

# Clean up
ru.close()
```

## RedisUtilities

High-performance Redis Streams client with connection pooling and batch operations.

### Initialization

```python
from aia_utilities import RedisUtilities

# Default configuration
ru = RedisUtilities()

# Custom configuration
ru = RedisUtilities(
    host='redis.example.com',
    port=6380,
    db=2,
    max_connections=20,
    decode_responses=False
)
```

### Methods

#### Writing Data

```python
# Write a single entry
entry_id = ru.write('stream_name', {'key': 'value'}, maxlen=10000)

# Batch write (faster for bulk inserts)
entries = [{'id': 1, 'data': 'a'}, {'id': 2, 'data': 'b'}]
count = ru.write_batch('stream_name', entries, maxlen=10000)
```

#### Reading Data

```python
# Read all entries (sorted by timestamp)
all_entries = ru.read_all('stream_name', order=True)

# Read limited entries
recent = ru.read_all('stream_name', count=100)

# Stream new entries as they arrive (generator)
for entry in ru.read_each('stream_name', start_id='$'):  # '$' = only new entries
    process(entry)

# Get latest entry matching field=value
latest = ru.get_latest('stream_name', 'symbol', 'AAPL')
```

#### Deleting Data

```python
# Delete entries matching a field value
deleted_count = ru.clear('stream_name', 'symbol', 'AAPL')

# Delete entire stream
success = ru.delete('stream_name')
```

#### Inspection

```python
# Print stream info and sample entries
ru.show('stream_name', sample=5)

# Get stream metadata
info = ru.get_stream_info('stream_name')
# Returns: {'length': 100, 'first_entry': ..., 'last_entry': ..., 'groups': 0}
```

### Data Format

All data is stored as JSON in stream entries. Your dicts are serialized automatically:

```python
# What you write
ru.write('prices', {'symbol': 'AAPL', 'price': 150.25})

# How it's stored in Redis
# Stream entry: {b'data': b'{"symbol": "AAPL", "price": 150.25}'}
```

## TimeManagement

Utilities for timezone conversions between UTC and America/New_York.

```python
from aia_utilities import TimeManagement

tm = TimeManagement()

# Convert UTC to New York time
ny_time = tm.utc_to_ny('2026-01-15T23:00:00Z')
# Returns: '2026-01-15 18:00:00.000000'

# Get UTC offset for a date
offset = tm.get_ny_utc_offset('2026-01-15 12:00:00')
# Returns: -5 (EST) or -4 (EDT depending on date)

# Convert datetime to string
timestamp_str = tm.datetime_to_string(datetime.now())
```

## Helpers

```python
from aia_utilities import Helpers

h = Helpers()

# Text-to-speech (macOS, non-blocking)
h.say_nonblocking("Hello world", voice="Samantha", volume=50)

# Direction label
h.updown(0.5)   # Returns: 'up'
h.updown(-0.3)  # Returns: 'down'
h.updown(0)     # Returns: None
```

## Testing

```bash
# Unit tests (mocked Redis)
pytest tests/test_redis_utilities.py -v

# Integration tests (requires running Redis)
pytest tests/test_redis_integration.py -v -s
```

## Requirements

- Python 3.10+
- Redis server (for production use)
- Dependencies: `redis`, `pandas`, `pytz`

## Building and Publishing to PyPI

1. Build distributions:

```bash
python -m pip install --upgrade build twine
python -m build
```

2. Upload to PyPI:

```bash
python -m twine upload dist/*
```

3. (Optional) Test upload to Test PyPI first:

```bash
python -m twine upload --repository testpypi dist/*
```

### Authentication

Use API tokens (recommended):

```bash
export TWINE_USERNAME='__token__'
export TWINE_PASSWORD='pypi-your-token-here'
python -m twine upload dist/*
```

Or configure `~/.pypirc`:

```ini
[pypi]
username = __token__
password = pypi-your-token-here
```

## License

MIT
