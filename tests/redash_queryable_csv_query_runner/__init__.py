import sys

from mock import MagicMock

sys.modules['redash'] = MagicMock()
sys.modules['redash.query_runner'] = MagicMock()
sys.modules['redash.query_runner'].TYPE_INTEGER = 'integer'
sys.modules['redash.query_runner'].TYPE_FLOAT = 'float'
sys.modules['redash.query_runner'].TYPE_STRING = 'string'
sys.modules['redash.query_runner'].register = MagicMock()
sys.modules['redash.query_runner'].BaseSQLQueryRunner = MagicMock()
sys.modules['redash.utils'] = MagicMock()
