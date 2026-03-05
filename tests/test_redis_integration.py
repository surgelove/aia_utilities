"""Integration tests for RedisUtilities with real Redis connection."""

import pytest
from aia_utilities.aia_utilities import RedisUtilities


@pytest.fixture
def redis_utils():
    """Create a real RedisUtilities instance."""
    utils = RedisUtilities(host='localhost', port=6379, db=0)
    yield utils
    utils.close()


class TestRealRedisStream:
    """Integration tests with real Redis."""

    def test_read_prices_stream(self, redis_utils):
        """Read from the real 'prices' stream."""
        entries = redis_utils.read_all('prices', order=False)
        
        print(f"\n{'='*50}")
        print(f"Found {len(entries)} entries in 'prices' stream")
        print(f"{'='*50}")
        
        if entries:
            print(f"\nFirst entry: {entries[0]}")
            print(f"Last entry: {entries[-1]}")
            
            # Show keys in entries
            if entries[0]:
                print(f"\nEntry keys: {list(entries[0].keys())}")
        else:
            print("Stream is empty or doesn't exist")
        
        # Just verify we got a list back
        assert isinstance(entries, list)

    def test_show_prices_stream(self, redis_utils, capsys):
        """Show sample of prices stream."""
        redis_utils.show('prices', sample=3)
        captured = capsys.readouterr()
        print(captured.out)

    def test_get_stream_info(self, redis_utils):
        """Get metadata about prices stream."""
        info = redis_utils.get_stream_info('prices')
        
        if info:
            print(f"\nStream info: {info}")
        else:
            print("Could not get stream info (stream may not exist)")
