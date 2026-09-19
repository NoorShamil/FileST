import pytest

from conftest import assert_ring_links, expected_successor, run_maintenance, sorted_ring

pytestmark = pytest.mark.week3


def test_single_node_is_a_ring_of_one(cluster):
    node = cluster.start_node()
    node.join()
    run_maintenance(cluster.nodes)

    assert node.successor.id == node.id
    assert node.predecessor.id == node.id


def test_three_nodes_form_a_correct_ring(cluster):
    a = cluster.start_node()
    a.join()
    b = cluster.start_node()
    b.join(a)
    c = cluster.start_node()
    c.join(a)
    run_maintenance(cluster.nodes, rounds=20)

    assert_ring_links(cluster.nodes)


def test_join_populates_successor_and_predecessor(cluster):
    bootstrap = cluster.start_node()
    bootstrap.join()
    new_node = cluster.start_node()
    new_node.join(bootstrap.host, bootstrap.port)
    run_maintenance(cluster.nodes, rounds=12)

    assert new_node.successor is not None
    assert new_node.predecessor is not None
    assert new_node.successor.id != new_node.id or len(cluster.nodes) == 1
    assert new_node.predecessor.id != new_node.id or len(cluster.nodes) == 1


def test_find_successor_matches_ring_ownership(cluster):
    nodes = cluster.start_nodes(6)
    nodes[0].join()
    for node in nodes[1:]:
        node.join(nodes[0].host, nodes[0].port)
    run_maintenance(cluster.nodes, rounds=24)

    for key_id in range(256):
        expected = expected_successor(nodes, key_id)
        actual = nodes[0].find_successor(key_id)
        assert actual.id == expected.id, f"wrong successor for key {key_id}"


def test_find_predecessor_is_immediate_predecessor(cluster):
    nodes = cluster.start_nodes(5)
    nodes[0].join()
    for node in nodes[1:]:
        node.join(nodes[0].host, nodes[0].port)
    run_maintenance(cluster.nodes, rounds=24)

    for key_id in range(256):
        succ = expected_successor(nodes, key_id)
        pred = succ.predecessor
        actual = nodes[0].find_predecessor(key_id)
        assert actual.id == pred.id


def test_successor_and_predecessor_links_are_reciprocal(cluster):
    nodes = cluster.start_nodes(7)
    nodes[0].join()
    for node in nodes[1:]:
        node.join(nodes[0].host, nodes[0].port)
    run_maintenance(cluster.nodes, rounds=24)

    for node in nodes:
        assert node.successor.predecessor.id == node.id
        assert node.predecessor.successor.id == node.id


def test_new_node_is_inserted_in_correct_position(cluster):
    nodes = cluster.start_nodes(3)
    nodes[0].join()
    nodes[1].join(nodes[0].host, nodes[0].port)
    nodes[2].join(nodes[0].host, nodes[0].port)
    run_maintenance(cluster.nodes, rounds=20)

    before = sorted_ring(nodes)
    newcomer = cluster.start_node()
    newcomer.join(nodes[0].host, nodes[0].port)
    run_maintenance(cluster.nodes, rounds=24)

    assert_ring_links(cluster.nodes)
    ordered = sorted_ring(cluster.nodes)
    idx = ordered.index(newcomer)
    assert ordered[idx - 1].successor.id == newcomer.id
    assert newcomer.successor.id == ordered[(idx + 1) % len(ordered)].id
