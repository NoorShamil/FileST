import time

import pytest

from conftest import direct_rpc, expected_successor, run_maintenance, wait_until

pytestmark = pytest.mark.week7


def build_cluster(cluster, count=7):
    nodes = cluster.start_nodes(count)
    for node in nodes:
        node.replication_factor = 3
    nodes[0].join()
    for node in nodes[1:]:
        node.join(nodes[0].host, nodes[0].port)
    run_maintenance(nodes, rounds=30)
    return nodes


def live_nodes(nodes, dead):
    return [node for node in nodes if node is not dead]


def test_failed_successor_is_detected(cluster):
    nodes = build_cluster(cluster)
    victim = nodes[0].successor
    assert victim is not nodes[0]

    predecessor = victim.predecessor
    cluster.kill_node(victim)

    wait_until(
        lambda: predecessor.successor.id != victim.id,
        timeout=4.0,
        description="predecessor to detect failed successor",
    )


def test_ring_reconnects_after_single_node_failure(cluster):
    nodes = build_cluster(cluster)
    victim = nodes[3]
    cluster.kill_node(victim)
    survivors = live_nodes(nodes, victim)

    run_maintenance(survivors, rounds=35)

    ordered = sorted(survivors, key=lambda n: n.id)
    for i, node in enumerate(ordered):
        assert node.successor.id == ordered[(i + 1) % len(ordered)].id
        assert node.predecessor.id == ordered[(i - 1) % len(ordered)].id


def test_lookup_does_not_route_to_failed_node(cluster):
    nodes = build_cluster(cluster)
    victim = nodes[2]
    cluster.kill_node(victim)
    survivors = live_nodes(nodes, victim)
    run_maintenance(survivors, rounds=30)

    for key_id in range(0, 256, 9):
        result = survivors[0].find_successor(key_id)
        assert result.id != victim.id


def test_replicated_data_remains_readable_after_primary_failure(cluster):
    nodes = build_cluster(cluster)
    key = "failure-survival"
    direct_rpc(nodes[1], {"type": "PUT", "key": key, "value": "alive"})

    key_id = nodes[0].storage.hash_key(key, nodes[0].m)
    owner = expected_successor(nodes, key_id)
    cluster.kill_node(owner)
    survivors = live_nodes(nodes, owner)
    run_maintenance(survivors, rounds=30)

    response = direct_rpc(survivors[0], {"type": "GET", "key": key})
    assert response["success"] is True
    assert response["value"] == "alive"


def test_new_primary_is_selected_after_failure(cluster):
    nodes = build_cluster(cluster)
    key = "promote-replica"
    direct_rpc(nodes[0], {"type": "PUT", "key": key, "value": "data"})

    owner = expected_successor(
        nodes,
        nodes[0].storage.hash_key(key, nodes[0].m),
    )
    expected_new_owner = owner.successor
    cluster.kill_node(owner)
    survivors = live_nodes(nodes, owner)
    run_maintenance(survivors, rounds=30)

    actual = expected_new_owner.find_successor(
        nodes[0].storage.hash_key(key, nodes[0].m)
    )
    assert actual.id == expected_new_owner.id


def test_cluster_recovers_when_failed_node_rejoins(cluster):
    nodes = build_cluster(cluster)
    victim = nodes[1]
    key = "rejoin-key"
    direct_rpc(nodes[0], {"type": "PUT", "key": key, "value": "before-rejoin"})

    cluster.kill_node(victim)
    survivors = live_nodes(nodes, victim)
    run_maintenance(survivors, rounds=30)

    assert hasattr(victim, "restart"), "Node.restart() is required for crash/rejoin recovery"
    victim.restart()

    from conftest import wait_until_listening
    wait_until_listening(victim.host, victim.port)
    victim.join(survivors[0].host, survivors[0].port)
    run_maintenance(survivors + [victim], rounds=35)

    response = direct_rpc(victim, {"type": "GET", "key": key})
    assert response["success"] is True
    assert response["value"] == "before-rejoin"


def test_get_handles_dead_node_without_hanging_forever(cluster):
    nodes = build_cluster(cluster)
    victim = nodes[4]
    key = "timeout-path"
    cluster.kill_node(victim)
    survivors = live_nodes(nodes, victim)
    run_maintenance(survivors, rounds=25)

    start = time.monotonic()
    response = direct_rpc(survivors[0], {"type": "GET", "key": key})
    elapsed = time.monotonic() - start

    assert response["success"] is False
    assert elapsed < 3.0
