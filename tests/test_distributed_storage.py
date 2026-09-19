from concurrent.futures import ThreadPoolExecutor

import pytest

from conftest import direct_rpc, expected_successor, run_maintenance

pytestmark = pytest.mark.week5


def build_cluster(cluster, count=6):
    nodes = cluster.start_nodes(count)
    nodes[0].join()
    for node in nodes[1:]:
        node.join(nodes[0].host, nodes[0].port)
    run_maintenance(nodes, rounds=24)
    return nodes


def test_put_through_non_owner_routes_to_owner(cluster):
    nodes = build_cluster(cluster)
    requester = nodes[0]
    key = "week5-routing-key"
    key_id = requester.storage.hash_key(key, requester.m)
    owner = expected_successor(nodes, key_id)

    response = direct_rpc(requester, {
        "type": "PUT",
        "key": key,
        "value": {"hello": "world"},
    })

    assert response["success"] is True
    assert response["node_id"] == owner.id
    assert owner.storage.get(key) == {"hello": "world"}

    for node in nodes:
        if node.id != owner.id:
            assert not node.storage.exists(key)


def test_get_through_arbitrary_node_routes_to_owner(cluster):
    nodes = build_cluster(cluster)
    key = "week5-get-routing"
    key_id = nodes[1].storage.hash_key(key, nodes[1].m)
    owner = expected_successor(nodes, key_id)
    owner.storage.put(key, "stored-value")

    requester = next(node for node in nodes if node.id != owner.id)
    response = direct_rpc(requester, {"type": "GET", "key": key})

    assert response["success"] is True
    assert response["value"] == "stored-value"
    assert response["node_id"] == owner.id


def test_delete_through_arbitrary_node_removes_owner_copy(cluster):
    nodes = build_cluster(cluster)
    key = "week5-delete-routing"
    owner = expected_successor(nodes, nodes[0].storage.hash_key(key, nodes[0].m))
    owner.storage.put(key, "to-delete")

    requester = next(node for node in nodes if node.id != owner.id)
    response = direct_rpc(requester, {"type": "DELETE", "key": key})

    assert response["success"] is True
    assert owner.storage.get(key) is None


def test_data_is_redistributed_when_a_node_joins(cluster):
    nodes = build_cluster(cluster, 3)
    original_nodes = list(nodes)

    # Find a key for which the newcomer becomes the owner after it joins.
    newcomer = cluster.start_node()
    key = None
    for i in range(10000):
        candidate = f"migration-{i}"
        candidate_id = original_nodes[0].storage.hash_key(candidate, original_nodes[0].m)
        old_owner = expected_successor(original_nodes, candidate_id)
        prospective = expected_successor(original_nodes + [newcomer], candidate_id)
        if prospective.id == newcomer.id and old_owner.id != newcomer.id:
            key = candidate
            break

    assert key is not None, "could not find a key owned by the joining node"

    old_owner = expected_successor(
        original_nodes,
        original_nodes[0].storage.hash_key(key, original_nodes[0].m),
    )
    old_owner.storage.put(key, "migrate-me")

    newcomer.join(original_nodes[0].host, original_nodes[0].port)
    run_maintenance(cluster.nodes, rounds=30)

    assert newcomer.storage.get(key) == "migrate-me"
    assert old_owner.storage.get(key) is None


def test_missing_key_is_routed_and_reported(cluster):
    nodes = build_cluster(cluster)
    response = direct_rpc(nodes[2], {"type": "GET", "key": "definitely-missing"})
    assert response["success"] is False
    assert response["value"] is None


def test_concurrent_distributed_puts(cluster):
    nodes = build_cluster(cluster, 8)
    items = [(f"distributed-{i}", {"i": i}) for i in range(40)]

    def put(item):
        key, value = item
        requester = nodes[value["i"] % len(nodes)]
        return direct_rpc(requester, {"type": "PUT", "key": key, "value": value})

    with ThreadPoolExecutor(max_workers=12) as pool:
        responses = list(pool.map(put, items))

    assert all(response["success"] for response in responses)

    for key, value in items:
        key_id = nodes[0].storage.hash_key(key, nodes[0].m)
        owner = expected_successor(nodes, key_id)
        assert owner.storage.get(key) == value
