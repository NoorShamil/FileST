import socket
import threading

import pytest

from protocol import HEADER_SIZE, send_message, receive_exact, receive_message


def test_message_round_trip_with_nested_and_unicode_data():
    left, right = socket.socketpair()
    try:
        message = {
            'type': 'PUT',
            'key': 'résumé 📄',
            'value': {'skills': ['Python', 'C#'], 'count': 42},
        }

        sender = threading.Thread(target=send_message, args=(left, message))
        sender.start()
        assert receive_message(right) == message
        sender.join(timeout=1)
        assert not sender.is_alive()
    finally:
        left.close()
        right.close()


def test_large_message_is_framed_correctly():
    left, right = socket.socketpair()
    try:
        message = {'type': 'PUT', 'key': 'large', 'value': 'x' * 100_000}
        send_message(left, message)
        assert receive_message(right) == message
    finally:
        left.close()
        right.close()


def test_receive_exact_raises_when_peer_closes_early():
    left, right = socket.socketpair()
    try:
        left.sendall(b'abc')
        left.close()
        with pytest.raises(ConnectionError):
            receive_exact(right, 4)
    finally:
        right.close()
