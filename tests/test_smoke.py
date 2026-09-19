import pytest

from conftest import direct_rpc, expected_successor, run_maintenance

pytestmark = pytest.mark.integration


def test_end_to_end_join_route_store_replicate(cluster):
    nodes = cluster.start_nodes(6)
    for node in nodes:
        node.replication_factor = 3
    nodes[0].join()
    for node in nodes[1:]:
        node.join(nodes[0].host, nodes[0].port)
    run_maintenance(nodes, rounds=30)

    key = "end-to-end"
    value = {"project": "ChordFS", "week": 7}
    response = direct_rpc(nodes[-1], {
        "type": "PUT",
        "key": key,
        "value": value,
    })
    assert response["success"] is True

    key_id = nodes[0].storage.hash_key(key, nodes[0].m)
    owner = expected_successor(nodes, key_id)
    assert owner.storage.get(key) == value

    get_response = direct_rpc(nodes[2], {"type": "GET", "key": key})
    assert get_response["success"] is True
    assert get_response["value"] == value

    # Verify the replicated copies exist before inducing a failure.
    replica_nodes = []
    current = owner
    for _ in range(3):
        replica_nodes.append(current)
        current = current.successor
    assert all(n.storage.get(key) == value for n in replica_nodes)

    cluster.kill_node(owner)
    survivors = [n for n in nodes if n is not owner]
    run_maintenance(survivors, rounds=30)

    failover_response = direct_rpc(survivors[-1], {"type": "GET", "key": key})
    assert failover_response["success"] is True
    assert failover_response["value"] == value
