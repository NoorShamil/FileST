from storage import Storage


def test_hash_is_deterministic_and_in_identifier_space(tmp_path):
    first = Storage.hash_key('hello', m=8)
    second = Storage.hash_key('hello', m=8)
    assert first == second
    assert 0 <= first < 256


def test_hash_respects_configured_identifier_space():
    assert 0 <= Storage.hash_key('hello', m=4) < 16
    assert 0 <= Storage.hash_key('hello', m=12) < 4096


def test_put_get_round_trip(tmp_path):
    storage = Storage(10, base_directory=tmp_path)
    storage.put('hello', 'world')
    assert storage.get('hello') == 'world'
    assert storage.exists('hello')


def test_put_overwrites_existing_value(tmp_path):
    storage = Storage(10, base_directory=tmp_path)
    storage.put('hello', 'world')
    storage.put('hello', 'changed')
    assert storage.get('hello') == 'changed'


def test_get_missing_key_returns_none(tmp_path):
    storage = Storage(10, base_directory=tmp_path)
    assert storage.get('missing') is None
    assert not storage.exists('missing')


def test_delete_existing_key(tmp_path):
    storage = Storage(10, base_directory=tmp_path)
    storage.put('hello', 'world')
    assert storage.delete('hello') is True
    assert storage.get('hello') is None
    assert not storage.exists('hello')


def test_delete_missing_key_returns_false(tmp_path):
    storage = Storage(10, base_directory=tmp_path)
    assert storage.delete('missing') is False


def test_different_keys_use_different_paths(tmp_path):
    storage = Storage(10, base_directory=tmp_path)
    assert storage.get_path('hello') != storage.get_path('goodbye')


def test_node_storage_directories_are_isolated(tmp_path):
    first = Storage(10, base_directory=tmp_path)
    second = Storage(20, base_directory=tmp_path)
    first.put('hello', 'from node 10')
    assert first.get('hello') == 'from node 10'
    assert second.get('hello') is None
