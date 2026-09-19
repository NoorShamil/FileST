import pytest

from conftest import direct_rpc, run_maintenance

pytestmark = pytest.mark.week3


def build_cluster(cluster, count=5):
    nodes = cluster.start_nodes(count)
    nodes[0].join()
    for node in nodes[1:]:
        node.join(nodes[0].host, nodes[0].port)
    run_maintenance(nodes, rounds=24)
    return nodes


def test_find_successor_message_returns_node_descriptor(cluster):
    nodes = build_cluster(cluster)
    key_id = 123
    response = direct_rpc(nodes[0], {
        "type": "FIND_SUCCESSOR",
        "key_id": key_id,
    })

    assert response["type"] == "FIND_SUCCESSOR_RESPONSE"
    assert isinstance(response["node"], dict)
    assert {"id", "host", "port"} <= set(response["node"])
    assert response["node"]["id"] == nodes[0].find_successor(key_id).id


def test_get_successor_message(cluster):
    nodes = build_cluster(cluster)
    response = direct_rpc(nodes[0], {"type": "GET_SUCCESSOR"})
    assert response["type"] == "SUCCESSOR_RESPONSE"
    assert response["node"]["id"] == nodes[0].successor.id


def test_get_predecessor_message(cluster):
    nodes = build_cluster(cluster)
    response = direct_rpc(nodes[0], {"type": "GET_PREDECESSOR"})
    assert response["type"] == "PREDECESSOR_RESPONSE"
    assert response["node"]["id"] == nodes[0].predecessor.id


def test_notify_message_is_accepted(cluster):
    nodes = build_cluster(cluster)
    response = direct_rpc(nodes[0], {
        "type": "NOTIFY",
        "node": {
            "id": nodes[1].id,
            "host": nodes[1].host,
            "port": nodes[1].port,
        },
    })
    assert response["type"] in {"NOTIFY_RESPONSE", "OK"}
