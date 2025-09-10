import json
import redis
import time
import uuid
from datetime import datetime
import subprocess
import threading
import pandas as pd
import pytz
from zoneinfo import ZoneInfo

TZ = ZoneInfo("America/New_York")

class RedisUtilities:
    """
    Utility class for interacting with Redis, including reading and writing JSON entries.
    """

    def __init__(self, host='localhost', port=6379, db=0):
        """
        Initialize the Redis_Utilities instance.

        Args:
            host (str): Redis server host.
            port (int): Redis server port.
            db (int): Redis database number.
        """
        self.host = host
        self.port = port
        self.db = db
        self.redis_db = redis.Redis(host=self.host, port=self.port, db=self.db)
        # This utility uses Redis Streams (XADD/XREAD/XRANGE) exclusively.
        # stream_maxlen controls approximate trimming when writing.

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

    def read_all(self, stream, order=True):
        """
        Read all Redis entries matching the given stream, returning them as a sorted list of dicts.

        Args:
            stream (str): Stream name to read from.
            order (bool): If True, sort results by 'timestamp'.

        Returns:
            list: List of dicts sorted by 'timestamp'.
        """

        """
        Read all entries from the Redis Stream named `stream` using XRANGE.

        Returns a list of dicts parsed from the stream 'data' field. If
        `order` is True and a 'timestamp' field exists it will sort by it
        (XRANGE already returns entries in ID order).
        """
        items = []
        try:
            entries = self.redis_db.xrange(stream, min='-', max='+')
        except Exception as e:
            print(f"Error reading stream {stream}: {e}")
            return items

        for entry_id, fields in entries:
            raw = fields.get(b'data') or fields.get('data')
            if raw is None:
                continue
            if isinstance(raw, bytes):
                raw = raw.decode()
            try:
                data = json.loads(raw)
            except json.JSONDecodeError as e:
                print(f"JSON decode error for stream entry {entry_id}: {e}")
                continue
            items.append(data)

        if order:
            try:
                items.sort(key=lambda x: x.get('timestamp', ''))
            except Exception:
                pass

        return items

    def read_each(self, stream):
        """
        Continuously yield new Redis entries matching the given stream as dicts.
        Stream should be <stream>:<instrument>

        Args:
            stream (str): Stream name to read from.

        Yields:
            dict: Newly found Redis entry as a dict.
        """
        """
        Continuously yield new entries from the Redis Stream named `stream`.

        This uses XREAD to block for new entries. It starts at '0-0' which
        will emit existing entries first; if you want only new entries use
        'last_id="$"' when calling this function externally and adapt as
        needed. Returned items are decoded JSON dicts.
        """
        last_id = '0-0'
        while True:
            try:
                # block for up to 1000ms waiting for new entries
                resp = self.redis_db.xread({stream: last_id}, block=1000)
                if not resp:
                    continue
                for stream_key, entries in resp:
                    for entry_id, fields in entries:
                        raw = fields.get(b'data') or fields.get('data')
                        if raw is None:
                            continue
                        if isinstance(raw, bytes):
                            raw = raw.decode()
                        try:
                            data = json.loads(raw)
                        except json.JSONDecodeError as e:
                            print(f"JSON decode error for stream entry {entry_id}: {e}")
                            continue
                        last_id = entry_id
                        yield data
            except Exception as e:
                print(f"Error reading stream {stream}: {e}")
                time.sleep(0.5)

    def write(self, stream, value, maxlen):
        """
        Write a dict value to Redis with a generated key using the given stream.
        Stream should be <stream>:<instrument>

        Args:
            stream (str): Stream name for the entry.
            value (dict): Value to store as JSON.
        """
        assert isinstance(value, dict)
        try:
            # store JSON in the 'data' field; approximate trimming to keep stream size bounded
            self.redis_db.xadd(stream, {'data': json.dumps(value)}, maxlen=maxlen, approximate=True)
        except Exception as e:
            print(f"Error writing to stream {stream}: {e}")

    def show(self, stream, sample=5):
        return_dict = self.read_all(stream, order=False)

        # tell me if the return_dict is ordered by timestamp or not
        if return_dict:
            is_ordered = all(return_dict[i]["timestamp"] <= return_dict[i + 1]["timestamp"] for i in range(len(return_dict) - 1))
            if is_ordered:
                print("The items are ordered by timestamp.")
            else:
                print("The items are NOT ordered by timestamp.")
        else:
            print("The stream is empty.")
        
        print(f'{len(return_dict)} items, type {type(return_dict)}')

        if len(return_dict) < sample * 2:
            sample_previous = sample
            sample = max(1, len(return_dict) // 2)
            print(f"Adjusted sample from {sample_previous} to {sample} because stream is {len(return_dict)} items.")

        # print the first 5 items in the return_dict and the last 5 items
        print(f"First {sample}:")
        for item in return_dict[:sample]:
            print(item)
        print(f"Last {sample}:")
        for item in return_dict[-sample:]:
            print(item)

    def delete(self, stream):
        """
        Delete a specific Redis stream key.

        Args:
            stream (str): The name of the stream to delete.

        Returns:
            bool: True if the key was deleted, False if it did not exist or on error.
        """
        try:
            res = self.redis_db.delete(stream)
            # redis.delete returns number of keys removed (0 or 1)
            return bool(res)
        except Exception as e:
            print(f"Error deleting stream {stream}: {e}")
            return False

    def clear(self, stream, field, key):
        # remove entries with the given key from the stream
        try:
            entries = self.redis_db.xrange(stream, min='-', max='+')
            for entry_id, fields in entries:
                # decode id bytes -> str
                if isinstance(entry_id, bytes):
                    entry_id = entry_id.decode()

                # get the data field (may be bytes)
                raw = fields.get(b'data') or fields.get('data')
                if isinstance(raw, bytes):
                    raw = raw.decode()

                # parse json and read timestamp
                data = json.loads(raw)
                f = data.get(field)

                if f == key:
                    self.redis_db.xdel(stream, entry_id)

        except Exception as e:
            print(f"Error clearing entries from stream {stream}: {e}")

    def get_latest(self, stream, field, key):
        try:
            # Get all entries in reverse order (newest first)
            entries = self.redis_db.xrevrange(stream, min='-', max='+')
            
            for entry_id, fields in entries:
                # get the data field (may be bytes)
                raw = fields.get(b'data') or fields.get('data')
                if isinstance(raw, bytes):
                    raw = raw.decode()

                # parse json and check field
                data = json.loads(raw)
                f = data.get(field)

                if f == key:
                    return data

        except Exception as e:
            print(f"Error getting latest entry from stream {stream}: {e}")
        return None


class TimeBasedMovement:

    def __init__(self, range):
        """Create a TimeBasedMovement tracker.

        Args:
            range (int): Window in minutes to calculate movement over.
        """
        # Holds dicts: {'timestamp': <pd.Timestamp>, 'price': <float>}
        self.data = []
        self.range = range
        # Keep a reasonable maximum to avoid unbounded memory growth
        self.max_size = 500

    def add(self, timestamp, price):
        self.data.append(
            {
                "timestamp": timestamp,
                "price": price,
            }
        )
        
        # Remove oldest data if queue exceeds max size
        if len(self.data) > self.max_size:
            self.data.pop(0)

    def clear(self):
        """Reset the stored price history to empty."""
        # Simple reset to drop all stored points
        self.data = []

    def calc(self):
        # Calculate the movement of the price for the last 5 minutes
        if len(self.data) < 2:
            return 0.0

        # Get the price data for the last n minutes
        range_ago = self.data[-1]["timestamp"] - pd.Timedelta(minutes=self.range)
        relevant_data = [d for d in self.data if d["timestamp"] > range_ago]

        if not relevant_data:
            return 0.0

        # Calculate the price movement percentage
        start_price = relevant_data[0]["price"]
        end_price = relevant_data[-1]["price"]
        
        # Avoid division by zero
        if start_price == 0:
            return 0.0
            
        return ((end_price - start_price) / start_price) * 100
    

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
        
    # def datetime_to_string(dt):
    #     """
    #     Convert a datetime object to an ISO-8601 string.

    #     Args:
    #         dt (datetime): The datetime object to convert.

    #     Returns:
    #         str: The converted ISO-8601 string or None on error.
    #     """
    #     try:
    #         return dt.isoformat()
    #     except Exception as e:
    #         print(f"Error converting datetime to string: {e}")
    #         return None
        
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

