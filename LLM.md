# LLM Context: aia_utilities

This document provides context for AI assistants working with this codebase.

## Overview

`aia_utilities` is a Python package providing optimized utilities for Redis Streams and time management, designed for use across Aia microservices.

## Package Structure

```
src/aia_utilities/
├── __init__.py          # Package exports
├── aia_utilities.py     # Main utilities module
tests/
├── test_redis_utilities.py      # Unit tests (mocked)
├── test_redis_integration.py    # Integration tests (real Redis)
```

## Core Classes

### RedisUtilities

A high-performance Redis Streams client with connection pooling and batch operations.

**Key Features:**
- Connection pooling (configurable pool size)
- Pipelined batch writes and deletes
- JSON serialization/deserialization built-in
- Exponential backoff on errors
- Approximate stream trimming for bounded memory

**Initialization:**
```python
from aia_utilities import RedisUtilities

# Default: localhost:6379, db 0, pool of 10 connections
ru = RedisUtilities()

# Custom config
ru = RedisUtilities(host='redis.example.com', port=6380, db=2, max_connections=20)
```

**Methods:**

| Method | Description | Returns |
|--------|-------------|---------|
| `write(stream, value, maxlen)` | Write a dict to stream | Entry ID or None |
| `write_batch(stream, values, maxlen)` | Write multiple dicts using pipeline | Count written |
| `read_all(stream, order=True, count=None)` | Read all/N entries | List of dicts |
| `read_each(stream, start_id='0-0', block_ms=1000)` | Generator yielding new entries | Generator |
| `get_latest(stream, field, key)` | Get newest entry matching field=key | Dict or None |
| `clear(stream, field, key, batch_size=1000)` | Delete entries where field=key | Count deleted |
| `delete(stream)` | Delete entire stream | bool |
| `show(stream, sample=5)` | Print stream info and samples | None |
| `get_stream_info(stream)` | Get stream metadata | Dict or None |
| `close()` | Close connection pool | None |

**Stream Data Format:**
All data is stored as JSON in the `data` field of stream entries:
```
Stream entry: {b'data': b'{"timestamp": "2026-01-01", "price": 100.5}'}
```

**Common Usage Patterns:**

```python
# Write price data
ru.write('prices', {'timestamp': '2026-01-01 10:00:00', 'symbol': 'AAPL', 'price': 150.25}, maxlen=10000)

# Batch write (much faster for bulk inserts)
prices = [{'timestamp': t, 'price': p} for t, p in data]
ru.write_batch('prices', prices, maxlen=10000)

# Read all prices, sorted by timestamp
all_prices = ru.read_all('prices', order=True)

# Stream new prices as they arrive
for price in ru.read_each('prices', start_id='$'):  # '$' = only new entries
    process(price)

# Get latest price for a symbol
latest = ru.get_latest('prices', 'symbol', 'AAPL')

# Remove all entries for a symbol
deleted = ru.clear('prices', 'symbol', 'AAPL')
```

### TimeManagement

Utilities for timezone conversions between UTC and America/New_York.

**Methods:**
- `datetime_to_string(timestamp)` - Convert datetime to string with microseconds
- `convert_utc_to_ny(utc_time_str)` - Convert ISO-8601 UTC to naive NY datetime string
- `utc_to_ny(utc_date_string, with_microseconds=True)` - Convert UTC string to NY string
- `get_ny_utc_offset(date_string)` - Get UTC offset hours (-5 EST, -4 EDT)
- `string_to_datetime(date_string)` - Parse ISO-8601 string to datetime

### Helpers

Miscellaneous utility functions.

**Methods:**
- `say_nonblocking(text, voice=None, volume=2)` - macOS text-to-speech in background thread
- `updown(direction)` - Returns 'up', 'down', or None for numeric direction

## Testing

**Unit tests (mocked Redis):**
```bash
pytest tests/test_redis_utilities.py -v
```

**Integration tests (requires running Redis):**
```bash
pytest tests/test_redis_integration.py -v -s
```

## Dependencies

- `redis` - Redis client
- `pandas` - Data manipulation
- `pytz` - Timezone handling

## Common Tasks for AI Assistants

### Adding a new RedisUtilities method
1. Add method to `RedisUtilities` class in `src/aia_utilities/aia_utilities.py`
2. Use `_parse_entry_data()` and `_decode_entry_id()` helpers for consistency
3. Add corresponding unit test in `tests/test_redis_utilities.py`
4. Use `self.redis_db.pipeline()` for batch operations

### Modifying stream data format
The `data` field contains JSON. Always use `json.dumps()` for writing and `_parse_entry_data()` for reading.

### Error handling pattern
```python
try:
    result = self.redis_db.some_operation()
except Exception as e:
    print(f"Error description: {e}")
    return None  # or empty list, or 0, as appropriate
```

## Architecture Notes

- Uses Redis Streams exclusively (XADD, XREAD, XRANGE, XREVRANGE)
- Connection pooling via `redis.ConnectionPool`
- All public methods include type hints
- Stream trimming is approximate (`approximate=True`) for performance
