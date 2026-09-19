from __future__ import annotations

import socket
import sys
import threading
import time
from contextlib import ExitStack
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def wait_until_listening(host: str, port: int, timeout: float = 3.0) -> None:
    deadline = time.time() + timeout
    last_error = None
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=0.1):
                return
        except OSError as exc:
            last_error = exc
            time.sleep(0.02)
    raise TimeoutError(f"{host}:{port} did not start listening: {last_error}")


def wait_until(predicate, timeout: float = 5.0, interval: float = 0.05, description: str = "condition"):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            result = predicate()
            if result:
                return result
            last = result
        except Exception as exc:
            last = exc
        time.sleep(interval)
    raise AssertionError(f"Timed out waiting for {description}; last result={last!r}")


def run_maintenance(nodes, rounds: int = 12, delay: float = 0.03):
    """Run stabilize/fix_fingers/check_predecessor enough times for convergence."""
    for _ in range(rounds):
        for node in list(nodes):
            node.stabilize()
            node.fix_fingers()
            node.check_predecessor()
        time.sleep(delay)


class NodeCluster:
    def __init__(self, tmp_path, m: int = 8, start_count: int = 0):
        self.tmp_path = tmp_path
        self.m = m
        self.nodes = []
        self.threads = []

        if start_count:
            self.start_nodes(start_count)

    def start_node(self, port=None):
        from node import Node

        if port is None:
            port = free_port()
        node = Node("127.0.0.1", port, m=self.m)
        thread = threading.Thread(target=node.start, daemon=True)
        thread.start()
        wait_until_listening(node.host, node.port)
        self.nodes.append(node)
        self.threads.append(thread)
        return node

    def start_nodes(self, count: int):
        for _ in range(count):
            self.start_node()
        return self.nodes

    def join(self, node, bootstrap=None):
        if bootstrap is None:
            node.join()
        else:
            node.join(bootstrap.host, bootstrap.port)
        run_maintenance(self.nodes)
        return node

    def stop_node(self, node, join_thread: bool = True):
        try:
            node.stop()
        finally:
            if join_thread:
                idx = self.nodes.index(node)
                self.threads[idx].join(timeout=1.5)

    def kill_node(self, node):
        """Simulate a crash without graceful leave/reorganization."""
        node.running = False
        if node.server_socket:
            try:
                node.server_socket.close()
            except OSError:
                pass
        idx = self.nodes.index(node)
        self.threads[idx].join(timeout=1.5)

    def close(self):
        for node in reversed(self.nodes):
            try:
                node.stop()
            except Exception:
                pass
        for thread in self.threads:
            thread.join(timeout=1.5)


@pytest.fixture
def running_node(tmp_path, monkeypatch):
    from node import Node
    monkeypatch.chdir(tmp_path)
    port = free_port()
    node = Node("127.0.0.1", port)
    thread = threading.Thread(target=node.start, daemon=True)
    thread.start()
    wait_until_listening(node.host, node.port)
    yield node
    node.stop()
    thread.join(timeout=1)


@pytest.fixture
def cluster(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    c = NodeCluster(tmp_path)
    yield c
    c.close()


def sorted_ring(nodes):
    return sorted(nodes, key=lambda n: n.id)


def expected_successor(nodes, key_id):
    ordered = sorted_ring(nodes)
    for node in ordered:
        if key_id <= node.id:
            return node
    return ordered[0]


def assert_ring_links(nodes):
    ordered = sorted_ring(nodes)
    for i, node in enumerate(ordered):
        expected_succ = ordered[(i + 1) % len(ordered)]
        expected_pred = ordered[(i - 1) % len(ordered)]
        assert node.successor.id == expected_succ.id
        assert node.predecessor.id == expected_pred.id


def direct_rpc(node, message):
    
    from node import Node
    client = Node("127.0.0.1", 0, m=node.m)
    return client.send_request(node.host, node.port, message)
