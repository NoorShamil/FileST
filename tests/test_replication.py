import pytest

from conftest import direct_rpc, expected_successor, run_maintenance

pytestmark = pytest.mark.week6


def build_cluster(cluster, count=7):
    nodes = cluster.start_nodes(count)
    for node in nodes:
        node.replication_factor = 3
    nodes[0].join()
    for node in nodes[1:]:
        node.join(nodes[0].host, nodes[0].port)
    run_maintenance(nodes, rounds=30)
    return nodes


def ring_successors(owner, count):
    result = []
    current = owner
    for _ in range(count):
        result.append(current)
        current = current.successor
    return result


def test_replication_factor_is_respected_on_put(cluster):
    nodes = build_cluster(cluster)
    requester = nodes[1]
    key = "replicated-key"
    response = direct_rpc(requester, {
        "type": "PUT",
        "key": key,
        "value": "replicated-value",
    })

    assert response["success"] is True
    key_id = requester.storage.hash_key(key, requester.m)
    owner = expected_successor(nodes, key_id)
    replicas = ring_successors(owner, 3)

    assert all(node.storage.get(key) == "replicated-value" for node in replicas)


def test_put_does_not_create_duplicate_replica_on_same_node(cluster):
    nodes = build_cluster(cluster, 5)
    key = "unique-replica-key"
    response = direct_rpc(nodes[0], {
        "type": "PUT",
        "key": key,
        "value": "x",
    })
    assert response["success"] is True

    locations = [node for node in nodes if node.storage.exists(key)]
    assert len({node.id for node in locations}) == len(locations)
    assert len(locations) == 3


def test_get_succeeds_when_primary_fails(cluster):
    nodes = build_cluster(cluster)
    key = "primary-failure-get"
    requester = nodes[0]
    direct_rpc(requester, {"type": "PUT", "key": key, "value": "survives"})

    key_id = requester.storage.hash_key(key, requester.m)
    owner = expected_successor(nodes, key_id)
    cluster.kill_node(owner)
    run_maintenance([n for n in nodes if n is not owner], rounds=25)

    survivors = [n for n in nodes if n is not owner]
    response = direct_rpc(survivors[0], {"type": "GET", "key": key})

    assert response["success"] is True
    assert response["value"] == "survives"


def test_delete_removes_all_replicas(cluster):
    nodes = build_cluster(cluster)
    key = "delete-all-replicas"
    direct_rpc(nodes[0], {"type": "PUT", "key": key, "value": "gone"})

    owner = expected_successor(
        nodes,
        nodes[0].storage.hash_key(key, nodes[0].m),
    )
    replicas = ring_successors(owner, 3)
    assert all(node.storage.exists(key) for node in replicas)

    response = direct_rpc(nodes[-1], {"type": "DELETE", "key": key})
    assert response["success"] is True

    assert all(not node.storage.exists(key) for node in replicas)


def test_replication_is_updated_for_overwrite(cluster):
    nodes = build_cluster(cluster)
    key = "overwrite-replica"
    direct_rpc(nodes[0], {"type": "PUT", "key": key, "value": "v1"})
    direct_rpc(nodes[-1], {"type": "PUT", "key": key, "value": "v2"})

    owner = expected_successor(
        nodes,
        nodes[0].storage.hash_key(key, nodes[0].m),
    )
    replicas = ring_successors(owner, 3)
    assert all(node.storage.get(key) == "v2" for node in replicas)


def test_cluster_with_fewer_nodes_than_replication_factor_does_not_duplicate_nodes(cluster):
    nodes = cluster.start_nodes(2)
    for node in nodes:
        node.replication_factor = 3
    nodes[0].join()
    nodes[1].join(nodes[0].host, nodes[0].port)
    run_maintenance(nodes, rounds=20)

    response = direct_rpc(nodes[1], {
        "type": "PUT",
        "key": "two-node-key",
        "value": "value",
    })
    assert response["success"] is True

    locations = [node for node in nodes if node.storage.exists("two-node-key")]
    assert len(locations) == 2
