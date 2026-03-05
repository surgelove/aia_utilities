"""Unit tests for RedisUtilities class."""

import json
import pytest
from unittest.mock import Mock, MagicMock, patch, call
from aia_utilities.aia_utilities import RedisUtilities


@pytest.fixture
def mock_redis():
    """Create a mock Redis client."""
    with patch('aia_utilities.aia_utilities.redis.ConnectionPool') as mock_pool, \
         patch('aia_utilities.aia_utilities.redis.Redis') as mock_redis_cls:
        mock_client = Mock()
        mock_redis_cls.return_value = mock_client
        yield mock_client


@pytest.fixture
def redis_utils(mock_redis):
    """Create a RedisUtilities instance with mocked Redis."""
    return RedisUtilities(host='localhost', port=6379, db=0)


class TestRedisUtilitiesInit:
    """Tests for RedisUtilities initialization."""

    def test_init_default_params(self, mock_redis):
        """Test initialization with default parameters."""
        utils = RedisUtilities()
        assert utils.host == 'localhost'
        assert utils.port == 6379
        assert utils.db == 0

    def test_init_custom_params(self, mock_redis):
        """Test initialization with custom parameters."""
        utils = RedisUtilities(host='redis.example.com', port=6380, db=5)
        assert utils.host == 'redis.example.com'
        assert utils.port == 6380
        assert utils.db == 5


class TestParseEntryData:
    """Tests for _parse_entry_data helper method."""

    def test_parse_bytes_data(self, redis_utils):
        """Test parsing data from bytes key."""
        fields = {b'data': b'{"key": "value", "num": 42}'}
        result = redis_utils._parse_entry_data(fields)
        assert result == {'key': 'value', 'num': 42}

    def test_parse_string_data(self, redis_utils):
        """Test parsing data from string key."""
        fields = {'data': '{"key": "value"}'}
        result = redis_utils._parse_entry_data(fields)
        assert result == {'key': 'value'}

    def test_parse_missing_data(self, redis_utils):
        """Test parsing when data field is missing."""
        fields = {'other': 'value'}
        result = redis_utils._parse_entry_data(fields)
        assert result is None

    def test_parse_invalid_json(self, redis_utils):
        """Test parsing invalid JSON returns None."""
        fields = {b'data': b'not valid json'}
        result = redis_utils._parse_entry_data(fields)
        assert result is None

    def test_parse_none_data(self, redis_utils):
        """Test parsing None data value."""
        fields = {b'data': None}
        result = redis_utils._parse_entry_data(fields)
        assert result is None


class TestDecodeEntryId:
    """Tests for _decode_entry_id helper method."""

    def test_decode_bytes_id(self, redis_utils):
        """Test decoding bytes entry ID."""
        entry_id = b'1234567890-0'
        result = redis_utils._decode_entry_id(entry_id)
        assert result == '1234567890-0'

    def test_decode_string_id(self, redis_utils):
        """Test string ID passes through unchanged."""
        entry_id = '1234567890-0'
        result = redis_utils._decode_entry_id(entry_id)
        assert result == '1234567890-0'


class TestReadAll:
    """Tests for read_all method."""

    def test_read_all_empty_stream(self, redis_utils, mock_redis):
        """Test reading from empty stream returns empty list."""
        mock_redis.xrange.return_value = []
        result = redis_utils.read_all('test_stream')
        assert result == []
        mock_redis.xrange.assert_called_once_with('test_stream', min='-', max='+', count=None)

    def test_read_all_with_entries(self, redis_utils, mock_redis):
        """Test reading entries from stream."""
        mock_redis.xrange.return_value = [
            (b'123-0', {b'data': b'{"id": 1, "timestamp": "2026-01-01"}'}),
            (b'124-0', {b'data': b'{"id": 2, "timestamp": "2026-01-02"}'}),
        ]
        result = redis_utils.read_all('test_stream')
        assert len(result) == 2
        assert result[0] == {'id': 1, 'timestamp': '2026-01-01'}
        assert result[1] == {'id': 2, 'timestamp': '2026-01-02'}

    def test_read_all_sorts_by_timestamp(self, redis_utils, mock_redis):
        """Test entries are sorted by timestamp when order=True."""
        mock_redis.xrange.return_value = [
            (b'123-0', {b'data': b'{"id": 1, "timestamp": "2026-01-02"}'}),
            (b'124-0', {b'data': b'{"id": 2, "timestamp": "2026-01-01"}'}),
        ]
        result = redis_utils.read_all('test_stream', order=True)
        assert result[0]['timestamp'] == '2026-01-01'
        assert result[1]['timestamp'] == '2026-01-02'

    def test_read_all_no_sort(self, redis_utils, mock_redis):
        """Test entries maintain order when order=False."""
        mock_redis.xrange.return_value = [
            (b'123-0', {b'data': b'{"id": 1, "timestamp": "2026-01-02"}'}),
            (b'124-0', {b'data': b'{"id": 2, "timestamp": "2026-01-01"}'}),
        ]
        result = redis_utils.read_all('test_stream', order=False)
        assert result[0]['timestamp'] == '2026-01-02'
        assert result[1]['timestamp'] == '2026-01-01'

    def test_read_all_with_count(self, redis_utils, mock_redis):
        """Test reading with count limit."""
        mock_redis.xrange.return_value = [
            (b'123-0', {b'data': b'{"id": 1}'}),
        ]
        redis_utils.read_all('test_stream', count=1)
        mock_redis.xrange.assert_called_once_with('test_stream', min='-', max='+', count=1)

    def test_read_all_skips_invalid_json(self, redis_utils, mock_redis):
        """Test invalid JSON entries are skipped."""
        mock_redis.xrange.return_value = [
            (b'123-0', {b'data': b'{"id": 1}'}),
            (b'124-0', {b'data': b'invalid json'}),
            (b'125-0', {b'data': b'{"id": 3}'}),
        ]
        result = redis_utils.read_all('test_stream')
        assert len(result) == 2

    def test_read_all_handles_exception(self, redis_utils, mock_redis):
        """Test exception handling returns empty list."""
        mock_redis.xrange.side_effect = Exception("Connection error")
        result = redis_utils.read_all('test_stream')
        assert result == []


class TestWrite:
    """Tests for write method."""

    def test_write_success(self, redis_utils, mock_redis):
        """Test successful write returns entry ID."""
        mock_redis.xadd.return_value = b'1234567890-0'
        result = redis_utils.write('test_stream', {'key': 'value'}, maxlen=1000)
        assert result == b'1234567890-0'
        mock_redis.xadd.assert_called_once_with(
            'test_stream',
            {'data': '{"key": "value"}'},
            maxlen=1000,
            approximate=True
        )

    def test_write_non_dict_raises_error(self, redis_utils):
        """Test writing non-dict raises TypeError."""
        with pytest.raises(TypeError, match="value must be a dict"):
            redis_utils.write('test_stream', "not a dict", maxlen=1000)

    def test_write_handles_exception(self, redis_utils, mock_redis):
        """Test exception handling returns None."""
        mock_redis.xadd.side_effect = Exception("Write error")
        result = redis_utils.write('test_stream', {'key': 'value'}, maxlen=1000)
        assert result is None


class TestWriteBatch:
    """Tests for write_batch method."""

    def test_write_batch_empty_list(self, redis_utils, mock_redis):
        """Test empty list returns 0."""
        result = redis_utils.write_batch('test_stream', [], maxlen=1000)
        assert result == 0

    def test_write_batch_success(self, redis_utils, mock_redis):
        """Test successful batch write."""
        mock_pipe = Mock()
        mock_pipe.execute.return_value = [b'123-0', b'124-0', b'125-0']
        mock_redis.pipeline.return_value = mock_pipe

        values = [{'id': 1}, {'id': 2}, {'id': 3}]
        result = redis_utils.write_batch('test_stream', values, maxlen=1000)

        assert result == 3
        assert mock_pipe.xadd.call_count == 3

    def test_write_batch_skips_non_dict(self, redis_utils, mock_redis):
        """Test non-dict values are skipped."""
        mock_pipe = Mock()
        mock_pipe.execute.return_value = [b'123-0']
        mock_redis.pipeline.return_value = mock_pipe

        values = [{'id': 1}, "not a dict", 123]
        redis_utils.write_batch('test_stream', values, maxlen=1000)

        # Only dict value should be added
        assert mock_pipe.xadd.call_count == 1

    def test_write_batch_handles_exception(self, redis_utils, mock_redis):
        """Test exception handling returns 0."""
        mock_pipe = Mock()
        mock_pipe.execute.side_effect = Exception("Pipeline error")
        mock_redis.pipeline.return_value = mock_pipe

        result = redis_utils.write_batch('test_stream', [{'id': 1}], maxlen=1000)
        assert result == 0


class TestDelete:
    """Tests for delete method."""

    def test_delete_existing_stream(self, redis_utils, mock_redis):
        """Test deleting existing stream returns True."""
        mock_redis.delete.return_value = 1
        result = redis_utils.delete('test_stream')
        assert result is True

    def test_delete_nonexistent_stream(self, redis_utils, mock_redis):
        """Test deleting non-existent stream returns False."""
        mock_redis.delete.return_value = 0
        result = redis_utils.delete('nonexistent')
        assert result is False

    def test_delete_handles_exception(self, redis_utils, mock_redis):
        """Test exception handling returns False."""
        mock_redis.delete.side_effect = Exception("Delete error")
        result = redis_utils.delete('test_stream')
        assert result is False


class TestClear:
    """Tests for clear method."""

    def test_clear_matching_entries(self, redis_utils, mock_redis):
        """Test clearing entries with matching field value."""
        mock_redis.xrange.return_value = [
            (b'123-0', {b'data': b'{"type": "a", "value": 1}'}),
            (b'124-0', {b'data': b'{"type": "b", "value": 2}'}),
            (b'125-0', {b'data': b'{"type": "a", "value": 3}'}),
        ]
        mock_pipe = Mock()
        mock_pipe.execute.return_value = [1, 1]  # Both entries deleted
        mock_redis.pipeline.return_value = mock_pipe

        result = redis_utils.clear('test_stream', 'type', 'a')

        assert result == 2
        assert mock_pipe.xdel.call_count == 2

    def test_clear_no_matches(self, redis_utils, mock_redis):
        """Test clearing when no entries match."""
        mock_redis.xrange.return_value = [
            (b'123-0', {b'data': b'{"type": "b", "value": 1}'}),
        ]

        result = redis_utils.clear('test_stream', 'type', 'a')
        assert result == 0

    def test_clear_empty_stream(self, redis_utils, mock_redis):
        """Test clearing empty stream."""
        mock_redis.xrange.return_value = []
        result = redis_utils.clear('test_stream', 'type', 'a')
        assert result == 0

    def test_clear_handles_exception(self, redis_utils, mock_redis):
        """Test exception handling returns 0."""
        mock_redis.xrange.side_effect = Exception("Clear error")
        result = redis_utils.clear('test_stream', 'type', 'a')
        assert result == 0

    def test_clear_respects_batch_size(self, redis_utils, mock_redis):
        """Test batching of deletions."""
        entries = [(f'{i}-0'.encode(), {b'data': b'{"type": "a"}'}) for i in range(5)]
        mock_redis.xrange.return_value = entries

        mock_pipe = Mock()
        mock_pipe.execute.return_value = [1, 1]  # Per batch
        mock_redis.pipeline.return_value = mock_pipe

        # With batch_size=2, should create 3 batches (2, 2, 1)
        redis_utils.clear('test_stream', 'type', 'a', batch_size=2)

        # Pipeline should be called 3 times (ceil(5/2))
        assert mock_redis.pipeline.call_count == 3


class TestGetLatest:
    """Tests for get_latest method."""

    def test_get_latest_found(self, redis_utils, mock_redis):
        """Test finding latest matching entry."""
        mock_redis.xrevrange.return_value = [
            (b'125-0', {b'data': b'{"type": "a", "value": 3}'}),  # Newest
            (b'124-0', {b'data': b'{"type": "b", "value": 2}'}),
            (b'123-0', {b'data': b'{"type": "a", "value": 1}'}),  # Oldest
        ]

        result = redis_utils.get_latest('test_stream', 'type', 'a')

        # Should return first match (newest with type 'a')
        assert result == {'type': 'a', 'value': 3}

    def test_get_latest_not_found(self, redis_utils, mock_redis):
        """Test no matching entry returns None."""
        mock_redis.xrevrange.return_value = [
            (b'123-0', {b'data': b'{"type": "b", "value": 1}'}),
        ]

        result = redis_utils.get_latest('test_stream', 'type', 'a')
        assert result is None

    def test_get_latest_empty_stream(self, redis_utils, mock_redis):
        """Test empty stream returns None."""
        mock_redis.xrevrange.return_value = []
        result = redis_utils.get_latest('test_stream', 'type', 'a')
        assert result is None

    def test_get_latest_handles_exception(self, redis_utils, mock_redis):
        """Test exception handling returns None."""
        mock_redis.xrevrange.side_effect = Exception("Read error")
        result = redis_utils.get_latest('test_stream', 'type', 'a')
        assert result is None


class TestGetStreamInfo:
    """Tests for get_stream_info method."""

    def test_get_stream_info_success(self, redis_utils, mock_redis):
        """Test getting stream info."""
        mock_redis.xinfo_stream.return_value = {
            'length': 100,
            'first-entry': (b'100-0', {b'data': b'{}'}),
            'last-entry': (b'200-0', {b'data': b'{}'}),
            'groups': 2,
        }

        result = redis_utils.get_stream_info('test_stream')

        assert result['length'] == 100
        assert result['groups'] == 2
        assert 'first_entry' in result
        assert 'last_entry' in result

    def test_get_stream_info_nonexistent(self, redis_utils, mock_redis):
        """Test non-existent stream returns None."""
        mock_redis.xinfo_stream.side_effect = Exception("Stream not found")
        result = redis_utils.get_stream_info('nonexistent')
        assert result is None


class TestShow:
    """Tests for show method."""

    def test_show_empty_stream(self, redis_utils, mock_redis, capsys):
        """Test showing empty stream."""
        mock_redis.xlen.return_value = 0
        redis_utils.show('test_stream')
        captured = capsys.readouterr()
        assert "The stream is empty." in captured.out

    def test_show_with_entries(self, redis_utils, mock_redis, capsys):
        """Test showing stream with entries."""
        mock_redis.xlen.return_value = 10
        mock_redis.xrange.return_value = [
            (b'123-0', {b'data': b'{"id": 1}'}),
            (b'124-0', {b'data': b'{"id": 2}'}),
        ]
        mock_redis.xrevrange.return_value = [
            (b'130-0', {b'data': b'{"id": 10}'}),
            (b'129-0', {b'data': b'{"id": 9}'}),
        ]

        redis_utils.show('test_stream', sample=2)
        captured = capsys.readouterr()

        assert "10 items in stream" in captured.out
        assert "First 2:" in captured.out
        assert "Last 2:" in captured.out

    def test_show_adjusts_sample_for_small_stream(self, redis_utils, mock_redis, capsys):
        """Test sample size adjustment for small streams."""
        mock_redis.xlen.return_value = 4
        mock_redis.xrange.return_value = [(b'123-0', {b'data': b'{"id": 1}'})]
        mock_redis.xrevrange.return_value = [(b'124-0', {b'data': b'{"id": 2}'})]

        redis_utils.show('test_stream', sample=5)
        captured = capsys.readouterr()

        assert "Adjusted sample from 5 to 2" in captured.out

    def test_show_handles_exception(self, redis_utils, mock_redis, capsys):
        """Test exception handling in show."""
        mock_redis.xlen.side_effect = Exception("Connection error")
        redis_utils.show('test_stream')
        captured = capsys.readouterr()
        assert "Error getting stream length" in captured.out


class TestReadEach:
    """Tests for read_each generator method."""

    def test_read_each_yields_entries(self, redis_utils, mock_redis):
        """Test read_each yields entries from stream."""
        # Return entries on first call, then empty to break out
        mock_redis.xread.return_value = [
            ('test_stream', [
                (b'123-0', {b'data': b'{"id": 1}'}),
                (b'124-0', {b'data': b'{"id": 2}'}),
            ])
        ]

        gen = redis_utils.read_each('test_stream')
        # Get first two entries
        result1 = next(gen)
        result2 = next(gen)

        assert result1 == {'id': 1}
        assert result2 == {'id': 2}

    def test_read_each_uses_start_id(self, redis_utils, mock_redis):
        """Test read_each uses custom start_id."""
        mock_redis.xread.return_value = [
            ('test_stream', [(b'123-0', {b'data': b'{"id": 1}'})])
        ]

        gen = redis_utils.read_each('test_stream', start_id='$')
        next(gen)

        mock_redis.xread.assert_called_with({'test_stream': '$'}, block=1000)

    def test_read_each_custom_block_ms(self, redis_utils, mock_redis):
        """Test read_each uses custom block_ms."""
        mock_redis.xread.return_value = [
            ('test_stream', [(b'123-0', {b'data': b'{"id": 1}'})])
        ]

        gen = redis_utils.read_each('test_stream', block_ms=5000)
        next(gen)

        mock_redis.xread.assert_called_with({'test_stream': '0-0'}, block=5000)

    def test_read_each_skips_invalid_json(self, redis_utils, mock_redis):
        """Test read_each skips entries with invalid JSON."""
        mock_redis.xread.return_value = [
            ('test_stream', [
                (b'123-0', {b'data': b'invalid json'}),
                (b'124-0', {b'data': b'{"id": 2}'}),
            ])
        ]

        gen = redis_utils.read_each('test_stream')
        result = next(gen)

        # Should skip invalid and return valid entry
        assert result == {'id': 2}


class TestStart:
    """Tests for start method."""

    def test_start_success(self, redis_utils, capsys):
        """Test successful Redis start."""
        with patch('subprocess.run') as mock_run:
            mock_run.return_value = None
            redis_utils.start()
            captured = capsys.readouterr()
            assert "Redis started" in captured.out

    def test_start_already_running(self, redis_utils, capsys):
        """Test Redis already running."""
        with patch('subprocess.run') as mock_run:
            from subprocess import CalledProcessError
            mock_run.side_effect = CalledProcessError(1, 'redis-server')
            redis_utils.start()
            captured = capsys.readouterr()
            assert "may already be running" in captured.out
