from concurrent.futures import ThreadPoolExecutor

from node import Node


def test_node_id_is_deterministic_and_in_range(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    a = Node('127.0.0.1', 5001, m=8)
    b = Node('127.0.0.1', 5001, m=8)
    assert a.id == b.id
    assert 0 <= a.id < 256


def test_ping_handler(running_node):
    node = running_node
    response = node.handle_message({'type': 'PING', 'sender_id': 123})
    assert response == {'type': 'PONG', 'node_id': node.id}


def test_put_handler_stores_value(running_node):
    response = running_node.handle_message({
        'type': 'PUT',
        'key': 'hello',
        'value': 'world',
    })
    assert response['type'] == 'PUT_RESPONSE'
    assert response['success'] is True
    assert response['key'] == 'hello'
    assert response['key_id'] == running_node.storage.hash_key('hello', running_node.m)
    assert response['node_id'] == running_node.id
    assert running_node.storage.get('hello') == 'world'


def test_get_handler_returns_stored_value(running_node):
    running_node.storage.put('hello', 'world')
    response = running_node.handle_message({'type': 'GET', 'key': 'hello'})
    assert response == {
        'type': 'GET_RESPONSE',
        'success': True,
        'key': 'hello',
        'value': 'world',
        'node_id': running_node.id,
    }


def test_get_handler_reports_missing_key(running_node):
    response = running_node.handle_message({'type': 'GET', 'key': 'missing'})
    assert response == {
        'type': 'GET_RESPONSE',
        'success': False,
        'key': 'missing',
        'value': None,
        'node_id': running_node.id,
    }


def test_delete_handler_removes_value(running_node):
    running_node.storage.put('hello', 'world')
    response = running_node.handle_message({'type': 'DELETE', 'key': 'hello'})
    assert response['type'] == 'DELETE_RESPONSE'
    assert response['success'] is True
    assert running_node.storage.get('hello') is None


def test_delete_missing_key_returns_false(running_node):
    response = running_node.handle_message({'type': 'DELETE', 'key': 'missing'})
    assert response['type'] == 'DELETE_RESPONSE'
    assert response['success'] is False


def test_missing_put_key_is_rejected(running_node):
    response = running_node.handle_message({'type': 'PUT', 'value': 'world'})
    assert response == {'type': 'ERROR', 'message': 'Missing key'}


def test_missing_put_value_is_rejected(running_node):
    response = running_node.handle_message({'type': 'PUT', 'key': 'hello'})
    assert response == {'type': 'ERROR', 'message': 'Missing value'}


def test_missing_get_key_is_rejected(running_node):
    response = running_node.handle_message({'type': 'GET'})
    assert response == {'type': 'ERROR', 'message': 'Missing key'}


def test_unknown_message_type_is_rejected(running_node):
    response = running_node.handle_message({'type': 'NOT_A_REAL_MESSAGE'})
    assert response == {
        'type': 'ERROR',
        'message': 'Unknown message type: NOT_A_REAL_MESSAGE',
    }


def test_tcp_ping_request(running_node):
    client = Node('127.0.0.1', 0, m=8)
    response = client.send_request(
        running_node.host,
        running_node.port,
        {'type': 'PING', 'sender_id': client.id},
    )
    assert response == {'type': 'PONG', 'node_id': running_node.id}


def test_tcp_put_get_delete_request_path(running_node):
    client = Node('127.0.0.1', 0, m=8)

    put_response = client.send_request(
        running_node.host,
        running_node.port,
        {'type': 'PUT', 'key': 'hello', 'value': 'world'},
    )
    assert put_response['success'] is True

    get_response = client.send_request(
        running_node.host,
        running_node.port,
        {'type': 'GET', 'key': 'hello'},
    )
    assert get_response['success'] is True
    assert get_response['value'] == 'world'

    delete_response = client.send_request(
        running_node.host,
        running_node.port,
        {'type': 'DELETE', 'key': 'hello'},
    )
    assert delete_response['success'] is True

    get_after_delete = client.send_request(
        running_node.host,
        running_node.port,
        {'type': 'GET', 'key': 'hello'},
    )
    assert get_after_delete['success'] is False


def test_concurrent_puts_are_handled(running_node):
    client = Node('127.0.0.1', 0, m=8)
    items = [(f'key-{i}', {'value': i}) for i in range(20)]

    def put(item):
        key, value = item
        return client.send_request(
            running_node.host,
            running_node.port,
            {'type': 'PUT', 'key': key, 'value': value},
        )

    with ThreadPoolExecutor(max_workers=10) as pool:
        responses = list(pool.map(put, items))

    assert all(response['success'] for response in responses)
    for key, value in items:
        assert running_node.storage.get(key) == value
