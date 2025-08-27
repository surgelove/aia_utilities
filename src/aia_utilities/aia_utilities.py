import json
import redis
import time


class Redis_Utilities:

    def __init__(self, host='localhost', port=6379, db=0):
        self.host = host
        self.port = port
        self.db = db

    def read_all(self, prefix, order=True):

        r = redis.Redis(host=self.host, port=self.port, db=self.db)

        # read existing keys and track seen ones
        seen = set()
        items = []
        for key in r.scan_iter(f"{prefix}"):
            seen.add(key)
            raw = r.get(key)
            if raw is None:
                continue
            # Convert raw JSON to dict
            try:
                data = json.loads(raw)
            except json.JSONDecodeError as e:
                print(f" JSON decode error for key={key}: {e}")
                continue

            items.append(data)

        # Sort items based on timestamp in the dict if requested
        if order:
            try:
                items.sort(key=lambda x: x["timestamp"])
            except Exception:
                pass

        return items

    def read_each(self, prefix):
        r = redis.Redis(host=self.host, port=self.port, db=self.db)
        seen = set()
        while True:
            time.sleep(0.1)
            for key in r.scan_iter(f"{prefix}"):
                if key not in seen:
                    seen.add(key)
                    raw = r.get(key)
                    if raw is None:
                        continue
                    try:
                        data = json.loads(raw)
                        yield data  # Send each new entry back to caller
                    except json.JSONDecodeError as e:
                        print(f"JSON decode error for key={key}: {e}")
                        continue


    def write(self, key, value):
        r = redis.Redis(host=self.host, port=self.port, db=self.db)
        r.set(key, json.dumps(value))


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
    
def string_to_datetime(date_string):
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

def datetime_to_string(dt):
    """
    Convert a datetime object to an ISO-8601 string.

    Args:
        dt (datetime): The datetime object to convert.

    Returns:
        str: The converted ISO-8601 string or None on error.
    """
    try:
        return dt.isoformat()
    except Exception as e:
        print(f"Error converting datetime to string: {e}")
        return None
    

def say_nonblocking(text, voice=None, volume=2):
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


def convert_utc_to_ny(utc_time_str):
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

def updown(direction):
    """Return a human-friendly direction label for a numeric delta.

    Args:
        direction (float): Positive for up, negative for down, zero for neutral.

    Returns:
        str or None: 'up', 'down', or None for no movement.
    """
    return "up" if direction > 0 else "down" if direction < 0 else None
