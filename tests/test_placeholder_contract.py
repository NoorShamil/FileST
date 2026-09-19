import inspect

import pytest

from node import Node


def test_required_public_methods_exist():
    required = [
        "join",
        "find_successor",
        "find_predecessor",
        "closest_preceding_node",
        "stabilize",
        "fix_fingers",
        "check_predecessor",
    ]
    missing = [name for name in required if not hasattr(Node, name)]
    assert not missing, f"Missing required Chord methods: {missing}"


def test_required_state_exists_after_construction(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    node = Node("127.0.0.1", 5001, m=8)
    missing = [
        name for name in ("successor", "predecessor", "finger_table")
        if not hasattr(node, name)
    ]
    assert not missing, f"Missing required Chord state: {missing}"


def test_required_maintenance_methods_take_no_arguments_except_self():
    for name in ("stabilize", "fix_fingers", "check_predecessor"):
        method = getattr(Node, name)
        params = list(inspect.signature(method).parameters)
        assert params == ["self"], f"{name} should be callable as node.{name}()"
