import json
import socket
import struct

HEADER_SIZE = 4
MAX_MESSAGE_SIZE = 16 * 1024 * 1024


def send_message(sock: socket.socket, message: dict) -> None:
    data = json.dumps(message).encode("utf-8")
    if len(data) > MAX_MESSAGE_SIZE:
        raise ValueError("message too large")
    header = struct.pack("!I", len(data))
    sock.sendall(header + data)


def receive_exact(sock: socket.socket, size: int) -> bytes:
    data = b""
    while len(data) < size:
        chunk = sock.recv(size - len(data))
        if not chunk:
            raise ConnectionError("Connection closed by peer")
        data += chunk
    return data


def receive_message(sock: socket.socket) -> dict:
    header = receive_exact(sock, HEADER_SIZE)
    message_length = struct.unpack("!I", header)[0]
    if message_length > MAX_MESSAGE_SIZE:
        raise ValueError("message too large")
    data = receive_exact(sock, message_length)
    return json.loads(data.decode("utf-8"))
