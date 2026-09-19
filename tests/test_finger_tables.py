import math
import pytest

from conftest import expected_successor, run_maintenance, sorted_ring

pytestmark = pytest.mark.week4


def build_cluster(cluster, count=8):
    nodes = cluster.start_nodes(count)
    nodes[0].join()
    for node in nodes[1:]:
        node.join(nodes[0].host, nodes[0].port)
    run_maintenance(nodes, rounds=30)
    return nodes


def test_finger_table_has_m_entries(cluster):
    nodes = build_cluster(cluster, 8)
    for node in nodes:
        assert len(node.finger_table) == node.m


def test_finger_table_entry_zero_is_successor(cluster):
    nodes = build_cluster(cluster, 8)
    for node in nodes:
        assert node.finger_table[0].id == node.successor.id


def test_each_finger_points_to_successor_of_its_start(cluster):
    nodes = build_cluster(cluster, 8)
    for node in nodes:
        for i, finger in enumerate(node.finger_table):
            start = (node.id + (2 ** i)) % (2 ** node.m)
            expected = expected_successor(nodes, start)
            assert finger.id == expected.id, (
                f"Node {node.id} finger[{i}] expected {expected.id} "
                f"for start {start}, got {finger.id}"
            )


def test_closest_preceding_node_returns_a_preceding_finger(cluster):
    nodes = build_cluster(cluster, 10)
    ring_size = 2 ** nodes[0].m

    for node in nodes:
        for key_id in range(0, ring_size, 7):
            candidate = node.closest_preceding_node(key_id)
            if key_id == node.id:
                continue
            # Candidate must lie strictly in (node.id, key_id) modulo the ring,
            # unless the node itself is the only valid option.
            distance_to_key = (key_id - node.id) % ring_size
            distance_candidate = (candidate.id - node.id) % ring_size
            assert 0 <= distance_candidate <= distance_to_key


def test_lookup_returns_correct_successor_for_every_key(cluster):
    nodes = build_cluster(cluster, 12)
    for key_id in range(256):
        expected = expected_successor(nodes, key_id)
        for node in nodes:
            actual = node.find_successor(key_id)
            assert actual.id == expected.id


def test_lookup_reports_reasonable_hop_count(cluster):
    nodes = build_cluster(cluster, 16)
    expected_bound = math.ceil(math.log2(len(nodes))) + 2

    for key_id in range(0, 256, 11):
        expected = expected_successor(nodes, key_id)
        result = nodes[0].find_successor(key_id, include_hops=True)
        actual = result.node if hasattr(result, 'node') else result['node']
        hops = result.hops if hasattr(result, 'hops') else result['hops']
        assert actual.id == expected.id
        assert hops <= expected_bound, (
            f"lookup for {key_id} used {hops} hops; bound={expected_bound}"
        )
