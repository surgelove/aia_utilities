import json
import redis
import time
from datetime import datetime
import subprocess
import threading
from typing import Any, Generator, Optional
import pandas as pd
import pytz
from zoneinfo import ZoneInfo

TZ = ZoneInfo("America/New_York")


class RedisUtilities:
    """
    Utility class for interacting with Redis Streams, with connection pooling
    and optimized batch operations.
    """

    def __init__(self, host: str = 'localhost', port: int = 6379, db: int = 0,
                 max_connections: int = 10, decode_responses: bool = False):
        """
        Initialize the RedisUtilities instance with connection pooling.

        Args:
            host: Redis server host.
            port: Redis server port.
            db: Redis database number.
            max_connections: Maximum connections in the pool.
            decode_responses: Whether to decode bytes to strings automatically.
        """
        self.host = host
        self.port = port
        self.db = db
        self._pool = redis.ConnectionPool(
            host=host, port=port, db=db,
            max_connections=max_connections,
            decode_responses=decode_responses
        )
        self.redis_db = redis.Redis(connection_pool=self._pool)

    def _parse_entry_data(self, fields: dict) -> Optional[dict]:
        """Parse JSON data from a stream entry's fields.

        Args:
            fields: The field dict from a stream entry.

        Returns:
            Parsed dict or None if parsing fails.
        """
        raw = fields.get(b'data') or fields.get('data')
        if raw is None:
            return None
        if isinstance(raw, bytes):
            raw = raw.decode()
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return None

    def _decode_entry_id(self, entry_id) -> str:
        """Decode entry ID to string if bytes."""
        return entry_id.decode() if isinstance(entry_id, bytes) else entry_id

    def start(self):
        """
        Start the Redis utility, including any necessary background tasks.
        """
        # Auto-start Redis if not running (dev only; assumes redis-server in PATH)
        try:
            subprocess.run(['redis-server', '--daemonize', 'yes'], check=True)
            print("✅ Redis started")
        except subprocess.CalledProcessError:
            print("⚠️ Redis may already be running or not installed")

    def read_all(self, stream: str, order: bool = True, count: Optional[int] = None) -> list[dict]:
        """
        Read all entries from a Redis Stream, returning them as a list of dicts.

        Args:
            stream: Stream name to read from.
            order: If True, sort results by 'timestamp'.
            count: Optional limit on number of entries to read.

        Returns:
            List of dicts, optionally sorted by 'timestamp'.
        """
        items = []
        try:
            entries = self.redis_db.xrange(stream, min='-', max='+', count=count)
        except Exception as e:
            print(f"Error reading stream {stream}: {e}")
            return items

        for _, fields in entries:
            data = self._parse_entry_data(fields)
            if data is not None:
                items.append(data)

        if order and items:
            items.sort(key=lambda x: x.get('timestamp', ''))

        return items

    def read_each(self, stream: str, start_id: str = '0-0',
                   block_ms: int = 1000) -> Generator[dict, None, None]:
        """
        Continuously yield new entries from a Redis Stream.

        Uses XREAD to block for new entries. Starts at start_id which defaults
        to '0-0' (all existing entries first). Use '$' for only new entries.

        Args:
            stream: Stream name to read from.
            start_id: Starting stream ID ('0-0' for all, '$' for new only).
            block_ms: Milliseconds to block waiting for entries.

        Yields:
            Newly found Redis entry as a dict.
        """
        last_id = start_id
        backoff = 0.5
        max_backoff = 30.0

        while True:
            try:
                resp = self.redis_db.xread({stream: last_id}, block=block_ms)
                backoff = 0.5  # Reset backoff on success

                if not resp:
                    continue

                for _, entries in resp:
                    for entry_id, fields in entries:
                        data = self._parse_entry_data(fields)
                        if data is not None:
                            last_id = entry_id
                            yield data

            except Exception as e:
                print(f"Error reading stream {stream}: {e}")
                time.sleep(backoff)
                backoff = min(backoff * 2, max_backoff)

    def write(self, stream: str, value: dict, maxlen: int) -> Optional[str]:
        """
        Write a dict value to a Redis Stream.

        Args:
            stream: Stream name for the entry.
            value: Value to store as JSON.
            maxlen: Approximate max stream length for trimming.

        Returns:
            The entry ID on success, None on error.
        """
        if not isinstance(value, dict):
            raise TypeError("value must be a dict")
        try:
            return self.redis_db.xadd(
                stream, {'data': json.dumps(value)},
                maxlen=maxlen, approximate=True
            )
        except Exception as e:
            print(f"Error writing to stream {stream}: {e}")
            return None

    def write_batch(self, stream: str, values: list[dict], maxlen: int = None) -> int:
        """
        Write multiple dict values to a Redis Stream using pipelining.

        Args:
            stream: Stream name for the entries.
            values: List of dicts to store as JSON.
            maxlen: Optional max stream length for trimming. None = no trimming.

        Returns:
            Number of successfully written entries.
        """
        if not values:
            return 0

        pipe = self.redis_db.pipeline()
        for value in values:
            if isinstance(value, dict):
                if maxlen is not None:
                    pipe.xadd(stream, {'data': json.dumps(value)},
                              maxlen=maxlen, approximate=True)
                else:
                    pipe.xadd(stream, {'data': json.dumps(value)})

        try:
            results = pipe.execute()
            return sum(1 for r in results if r is not None)
        except Exception as e:
            print(f"Error in batch write to stream {stream}: {e}")
            return 0

    def show(self, stream: str, sample: int = 5) -> None:
        """
        Display stream info and sample entries (first and last N).

        Args:
            stream: Stream name to inspect.
            sample: Number of entries to show from start and end.
        """
        try:
            length = self.redis_db.xlen(stream)
        except Exception as e:
            print(f"Error getting stream length: {e}")
            return

        if length == 0:
            print("The stream is empty.")
            return

        print(f'{length} items in stream')

        # Adjust sample size for small streams
        effective_sample = min(sample, max(1, length // 2))
        if effective_sample != sample:
            print(f"Adjusted sample from {sample} to {effective_sample} (stream has {length} items)")

        # Get first N entries efficiently
        first_entries = self.redis_db.xrange(stream, min='-', max='+', count=effective_sample)
        print(f"\nFirst {effective_sample}:")
        for _, fields in first_entries:
            data = self._parse_entry_data(fields)
            if data:
                print(data)

        # Get last N entries efficiently (reverse order)
        last_entries = self.redis_db.xrevrange(stream, min='-', max='+', count=effective_sample)
        print(f"\nLast {effective_sample}:")
        for _, fields in reversed(last_entries):
            data = self._parse_entry_data(fields)
            if data:
                print(data)

    def delete(self, stream: str) -> bool:
        """
        Delete a specific Redis stream key.

        Args:
            stream: The name of the stream to delete.

        Returns:
            True if the key was deleted, False if it did not exist or on error.
        """
        try:
            return bool(self.redis_db.delete(stream))
        except Exception as e:
            print(f"Error deleting stream {stream}: {e}")
            return False

    def clear(self, stream: str, field: str, key: Any, batch_size: int = 1000) -> int:
        """
        Remove entries with a matching field value from the stream using pipelining.

        Args:
            stream: Stream name to clear from.
            field: Field name to check in the JSON data.
            key: Value to match for deletion.
            batch_size: Number of deletions per pipeline batch.

        Returns:
            Number of entries deleted.
        """
        deleted = 0
        try:
            entries = self.redis_db.xrange(stream, min='-', max='+')
            ids_to_delete = []

            for entry_id, fields in entries:
                data = self._parse_entry_data(fields)
                if data and data.get(field) == key:
                    ids_to_delete.append(self._decode_entry_id(entry_id))

            # Batch delete using pipeline
            for i in range(0, len(ids_to_delete), batch_size):
                batch = ids_to_delete[i:i + batch_size]
                pipe = self.redis_db.pipeline()
                for eid in batch:
                    pipe.xdel(stream, eid)
                results = pipe.execute()
                deleted += sum(results)

        except Exception as e:
            print(f"Error clearing entries from stream {stream}: {e}")

        return deleted

    def get_latest(self, stream: str, field: str, key: Any) -> Optional[dict]:
        """
        Get the most recent entry matching a field value.

        Args:
            stream: Stream name to search.
            field: Field name to match.
            key: Value to match.

        Returns:
            The matching dict or None if not found.
        """
        try:
            entries = self.redis_db.xrevrange(stream, min='-', max='+')
            for _, fields in entries:
                data = self._parse_entry_data(fields)
                if data and data.get(field) == key:
                    return data
        except Exception as e:
            print(f"Error getting latest entry from stream {stream}: {e}")
        return None

    def get_stream_info(self, stream: str) -> Optional[dict]:
        """
        Get stream metadata including length, first/last entry info.

        Args:
            stream: Stream name to inspect.

        Returns:
            Dict with stream info or None on error.
        """
        try:
            info = self.redis_db.xinfo_stream(stream)
            return {
                'length': info.get('length', 0),
                'first_entry': info.get('first-entry'),
                'last_entry': info.get('last-entry'),
                'groups': info.get('groups', 0),
            }
        except Exception as e:
            print(f"Error getting stream info for {stream}: {e}")
            return None

    def close(self) -> None:
        """Close all connections in the pool."""
        self._pool.disconnect()

class TimeManagement:

    def __init__(self):
        pass

    def datetime_to_string(self, timestamp):
        timestamp_with_microseconds = timestamp.strftime("%Y-%m-%d %H:%M:%S.%f")
        return timestamp_with_microseconds
    
    def convert_utc_to_ny(self, utc_time_str):
        """Convert an ISO-8601 UTC timestamp (optionally ending with 'Z')
        into a naive datetime string in the America/New_York timezone.

        Returns a string formatted as YYYY-MM-DD HH:MM:SS or None on error.
        """
        try:
            return (datetime.fromisoformat(utc_time_str.replace('Z', '+00:00'))
                    .astimezone(pytz.timezone('America/New_York'))
                    .replace(tzinfo=None, microsecond=0)
                    .strftime('%Y-%m-%d %H:%M:%S'))
        except Exception as e:
            print(f"Error converting time: {e}")
            return None

    def utc_to_ny(self, utc_date_string, with_microseconds=True):
        """
        Convert a UTC date string to New York time.
        
        Args:
            utc_date_string: UTC date string in formats like:
                            - "2025-01-15T23:00:00Z"
                            - "2025-01-15 23:00:00"
                            - "2025-07-15T22:00:00Z"
                            - "2025-10-19 22:00:57.778102"
        
        Returns:
            datetime string in format: "YYYY-MM-DD HH:MM:SS.ffffff"
        """
        # Define timezones
        utc = pytz.UTC
        ny_tz = pytz.timezone('America/New_York')
        
        # Parse the UTC string (handle both formats with/without 'Z')
        utc_date_string = utc_date_string.replace('Z', '')
        
        # Try different datetime formats (order matters - most specific first)
        formats = [
            "%Y-%m-%d %H:%M:%S.%f",      # 2025-10-19 22:00:57.778102
            "%Y-%m-%dT%H:%M:%S.%f",      # 2025-01-15T23:00:00.123456
            "%Y-%m-%d %H:%M:%S",         # 2025-01-15 23:00:00
            "%Y-%m-%dT%H:%M:%S",         # 2025-01-15T23:00:00
        ]
        
        dt_utc = None
        for fmt in formats:
            try:
                dt_utc = datetime.strptime(utc_date_string, fmt)
                break
            except ValueError:
                continue
        
        if dt_utc is None:
            raise ValueError(f"Unable to parse date string: {utc_date_string}")
        
        # Make it timezone-aware (UTC)
        dt_utc = utc.localize(dt_utc)
        
        # Convert to New York time
        dt_ny = dt_utc.astimezone(ny_tz)
        
        # Return formatted string with microseconds, without timezone
        if not with_microseconds:
            return dt_ny.strftime('%Y-%m-%d %H:%M:%S')
        return dt_ny.strftime('%Y-%m-%d %H:%M:%S.%f')   
        

    def get_ny_utc_offset(self, date_string):
        """
        Get the UTC offset for New York timezone at a given date.
        
        Args:
            date_string: Date string in various formats like:
                        - "2025-01-15 23:00:00"
                        - "2025-07-15T22:00:00"
                        - "2025-10-19 22:00:57.778102"
        
        Returns:
            int: Offset in hours (e.g., -5 for EST, -4 for EDT)
        """
        from datetime import datetime
        import pytz
        
        # Define timezones
        utc = pytz.UTC
        ny_tz = pytz.timezone('America/New_York')
        
        # Parse the date string (handle various formats)
        date_string = date_string.replace('Z', '')
        
        formats = [
            "%Y-%m-%d %H:%M:%S.%f",
            "%Y-%m-%dT%H:%M:%S.%f",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%dT%H:%M:%S",
        ]
        
        dt_utc = None
        for fmt in formats:
            try:
                dt_utc = datetime.strptime(date_string, fmt)
                break
            except ValueError:
                continue
        
        if dt_utc is None:
            raise ValueError(f"Unable to parse date string: {date_string}")
        
        # Make it timezone-aware (UTC)
        dt_utc = utc.localize(dt_utc)
        
        # Convert to New York time
        dt_ny = dt_utc.astimezone(ny_tz)
        
        # Get offset in hours
        offset_seconds = dt_ny.utcoffset().total_seconds()
        offset_hours = int(offset_seconds / 3600)
        
        return offset_hours

    def string_to_datetime(self, date_string):
        """
        Convert a string to a datetime object.

        Args:
            date_string (str): The date string to convert.

        Returns:
            datetime: The converted datetime object or None on error.
        """
        try:
            return datetime.fromisoformat(date_string.replace('Z', '+00:00'))
        except Exception as e:
            print(f"Error converting string to datetime: {e}")
            return None 

class Helpers:

    def say_nonblocking(self, text, voice=None, volume=2):
        """Speak `text` using the macOS `say` command without blocking.

        The speaking is performed in a daemon thread so the caller can
        continue execution immediately. Volume is set with `osascript`.

        Args:
            text (str): The text to speak.
            voice (str, optional): Voice name to pass to `say`.
            volume (int, optional): Volume level (0-100). Default 2 here
                matches the historical value in the repo; users can change it.
        """
        print("Speaking:", text)
        def speak():
            try:
                # Set system volume before speaking
                # This requires 'osascript' which is available on macOS
                volume_cmd = ['osascript', '-e', f'set volume output volume {volume}']
                subprocess.run(volume_cmd, check=True)
                
                # Now speak the text
                cmd = ['say']
                if voice:
                    cmd.extend(['-v', voice])
                cmd.append(text)
                subprocess.run(cmd, check=True)
                
                # Optional: Reset volume to a default level when done
                # subprocess.run(['osascript', '-e', 'set volume output volume 75'], check=True)
            except Exception as e:
                print(f"Error with text-to-speech: {e}")
        
        # Run in a separate thread to avoid blocking
        thread = threading.Thread(target=speak, daemon=True)
        thread.start()

    def updown(self, direction):
        """Return a human-friendly direction label for a numeric delta.

        Args:
            direction (float): Positive for up, negative for down, zero for neutral.

        Returns:
            str or None: 'up', 'down', or None for no movement.
        """
        return "up" if direction > 0 else "down" if direction < 0 else None

class Data:
    
    def __init__(self):
        pass

    def df_from_dicts(self, data):
        ...

class Broker:

    def __init__(self):
        pass