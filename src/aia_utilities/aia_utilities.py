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
