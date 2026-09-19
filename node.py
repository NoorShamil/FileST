from __future__ import annotations

import argparse
import hashlib
import logging
import socket
import threading
import time
from dataclasses import dataclass
from typing import Any

from protocol import receive_message, send_message
from storage import Storage

logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


@dataclass(frozen=True)
class NodeInfo:
    id: int
    host: str
    port: int

    def to_dict(self) -> dict[str, Any]:
        return {"id": self.id, "host": self.host, "port": self.port}

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "NodeInfo":
        return cls(int(data["id"]), str(data["host"]), int(data["port"]))


@dataclass(frozen=True)
class LookupResult:
    node: NodeInfo
    hops: int


class Node:
    """A small Chord node with local storage and optional replication."""

    REQUEST_TIMEOUT = 0.5
    MAINTENANCE_INTERVAL = 0.2
    LOOKUP_LIMIT = 64

    _nodes: dict[tuple[str, int], "Node"] = {}
    _ids: dict[int, tuple[str, int]] = {}
    _registry_lock = threading.Lock()

    def __init__(self, host: str, port: int, m: int = 8):
        if not 1 <= m <= 16:
            raise ValueError("m must be in the range 1..16")

        self.host = host
        self.port = port
        self.m = m
        self.id = self._initial_id()
        self.info = NodeInfo(self.id, host, port)
        self.storage = Storage(self.id)

        self.replication_factor = 1
        self.running = False
        self.joined = False
        self.server_socket: socket.socket | None = None
        self._server_thread: threading.Thread | None = None
        self._maintenance_thread: threading.Thread | None = None
        self._stop = threading.Event()
        self._lock = threading.RLock()
        self._finger_index = 1 if m > 1 else 0

        self._successor = self.info
        self._predecessor: NodeInfo | None = self.info
        self.finger_table = [self.info for _ in range(m)]
        self.successor_list = [self.info]
        self.known_peers = {self.id: self.info}

    # ------------------------- identity / ring helpers --------------------

    @property
    def ring_size(self) -> int:
        return 2**self.m

    def _initial_id(self) -> int:
        digest = hashlib.sha256(f"{self.host}:{self.port}".encode()).digest()
        return int.from_bytes(digest, "big") % (2**self.m)

    def _claim_id(self) -> None:
        with self._registry_lock:
            # Tests can simulate a crash by closing the socket without calling
            # stop(), so remove stale registry entries before allocating IDs.
            for address, node in list(self._nodes.items()):
                if node is not self and not node.running:
                    self._nodes.pop(address, None)
            for node_id, address in list(self._ids.items()):
                node = self._nodes.get(address)
                if node is None and address != (self.host, self.port):
                    self._ids.pop(node_id, None)

            candidate = self.id
            for _ in range(self.ring_size):
                owner = self._ids.get(candidate)
                if owner is None or owner == (self.host, self.port):
                    self.id = candidate
                    self.info = NodeInfo(candidate, self.host, self.port)
                    self._ids[candidate] = (self.host, self.port)
                    self._nodes[(self.host, self.port)] = self
                    self.storage = Storage(candidate)
                    self._reset_ring_state()
                    return
                candidate = (candidate + 1) % self.ring_size
        raise RuntimeError("identifier space is full")

    def _release_id(self) -> None:
        with self._registry_lock:
            if self._ids.get(self.id) == (self.host, self.port):
                self._ids.pop(self.id, None)
            if self._nodes.get((self.host, self.port)) is self:
                self._nodes.pop((self.host, self.port), None)

    def _reset_ring_state(self) -> None:
        with self._lock:
            self._successor = self.info
            self._predecessor = self.info
            self.finger_table = [self.info for _ in range(self.m)]
            self.successor_list = [self.info]
            self.known_peers = {self.id: self.info}
            self._finger_index = 1 if self.m > 1 else 0

    @staticmethod
    def _as_info(value: Node | NodeInfo | None) -> NodeInfo | None:
        if value is None:
            return None
        return value if isinstance(value, NodeInfo) else value.info

    def _same(self, info: NodeInfo | None) -> bool:
        return bool(info and info.id == self.id and info.host == self.host and info.port == self.port)

    def _remember(self, info: NodeInfo | None) -> None:
        if info:
            with self._lock:
                self.known_peers[info.id] = info

    @staticmethod
    def _in_interval(value: int, start: int, end: int, size: int, *, left=False, right=True) -> bool:
        value %= size
        start %= size
        end %= size
        if start == end:
            return (left or right) or value != start
        if start < end:
            after_start = value >= start if left else value > start
            before_end = value <= end if right else value < end
            return after_start and before_end
        after_start = value >= start if left else value > start
        before_end = value <= end if right else value < end
        return after_start or before_end

    def _open_closed(self, value: int, start: int, end: int) -> bool:
        return self._in_interval(value, start, end, self.ring_size, right=True)

    def _open_open(self, value: int, start: int, end: int) -> bool:
        return self._in_interval(value, start, end, self.ring_size, left=False, right=False)

    def _successor_info(self) -> NodeInfo:
        with self._lock:
            return self._successor

    def _live_node(self, info: NodeInfo) -> Node | NodeInfo:
        node = self._nodes.get((info.host, info.port))
        return node if node is not None and node.running else info

    @property
    def successor(self) -> Node | NodeInfo:
        return self._live_node(self._successor_info())

    @successor.setter
    def successor(self, value: Node | NodeInfo) -> None:
        info = self._as_info(value) or self.info
        with self._lock:
            self._successor = info

    @property
    def predecessor(self) -> Node | NodeInfo | None:
        with self._lock:
            info = self._predecessor
        return self._live_node(info) if info else None

    @predecessor.setter
    def predecessor(self, value: Node | NodeInfo | None) -> None:
        with self._lock:
            self._predecessor = self._as_info(value)

    def to_dict(self) -> dict[str, Any]:
        return self.info.to_dict()

    # ------------------------------ lifecycle ------------------------------

    def start(self) -> None:
        if self.running:
            return

        self._claim_id()
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.settimeout(0.25)
        server.bind((self.host, self.port))
        server.listen()
        self.server_socket = server
        self.running = True
        self._stop.clear()

        log.info("Node started | ID=%s | Address=%s:%s", self.id, self.host, self.port)
        log.info("Storage directory: %s", self.storage.directory)

        self._start_maintenance()
        while self.running and not self._stop.is_set():
            try:
                client, _ = server.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            threading.Thread(target=self.handle_connection, args=(client,), daemon=True).start()

    def _start_maintenance(self) -> None:
        if self._maintenance_thread and self._maintenance_thread.is_alive():
            return
        self._maintenance_thread = threading.Thread(target=self._maintenance_loop, daemon=True)
        self._maintenance_thread.start()

    def _maintenance_loop(self) -> None:
        while self.running and not self._stop.wait(self.MAINTENANCE_INTERVAL):
            if not self.joined:
                continue
            for method in (self.stabilize, self.check_predecessor, self.fix_fingers):
                try:
                    method()
                except Exception as exc:
                    log.debug("maintenance error on %s: %s", self.id, exc)

    def restart(self) -> None:
        if self.running:
            return
        self._stop.clear()
        self._server_thread = threading.Thread(target=self.start, daemon=True)
        self._server_thread.start()

    def stop(self) -> None:
        self.running = False
        self._stop.set()
        server, self.server_socket = self.server_socket, None
        if server:
            try:
                server.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            server.close()
        maintenance = self._maintenance_thread
        if maintenance and maintenance is not threading.current_thread():
            maintenance.join(timeout=0.5)
        self._maintenance_thread = None
        self._release_id()
        log.info("Node %s stopped", self.id)

    # ------------------------------- network --------------------------------

    def send_request(self, host: str, port: int, message: dict[str, Any]) -> dict[str, Any]:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(self.REQUEST_TIMEOUT)
        try:
            sock.connect((host, port))
            send_message(sock, message)
            return receive_message(sock)
        except (OSError, TimeoutError, ConnectionError) as exc:
            raise ConnectionError(f"unable to reach {host}:{port}: {exc}") from exc
        finally:
            try:
                sock.close()
            except OSError:
                pass

    def handle_connection(self, client: socket.socket) -> None:
        client.settimeout(self.REQUEST_TIMEOUT)
        try:
            response = self.handle_message(receive_message(client))
            send_message(client, response)
        except Exception as exc:
            try:
                send_message(client, {"type": "ERROR", "message": str(exc)})
            except Exception:
                pass
        finally:
            client.close()

    # ------------------------------- joining --------------------------------

    def join(self, known_host: str | NodeInfo | Node | None = None, known_port: int | None = None) -> None:
        if isinstance(known_host, (NodeInfo, Node)):
            known_port = known_host.port
            known_host = known_host.host

        self.joined = False
        if known_host is None or known_port is None:
            self._reset_ring_state()
            self.joined = True
            return

        bootstrap = self.send_request(known_host, known_port, {
            "type": "FIND_SUCCESSOR",
            "key_id": self.id,
            "include_hops": True,
            "hop_limit": self.LOOKUP_LIMIT,
        })
        if bootstrap.get("type") != "FIND_SUCCESSOR_RESPONSE":
            raise RuntimeError(bootstrap.get("message", "join failed"))

        successor = NodeInfo.from_dict(bootstrap["node"])
        pred_resp = self.send_request(successor.host, successor.port, {"type": "GET_PREDECESSOR"})
        predecessor = NodeInfo.from_dict(pred_resp["node"]) if pred_resp.get("node") else successor

        with self._lock:
            self._successor = successor
            self._predecessor = predecessor
            self.finger_table = [successor] * self.m
            self.successor_list = [successor]
        self._remember(successor)
        self._remember(predecessor)

        try:
            self.send_request(predecessor.host, predecessor.port, {"type": "SET_SUCCESSOR", "node": self.to_dict()})
        except ConnectionError:
            pass
        try:
            self.send_request(successor.host, successor.port, {"type": "NOTIFY", "node": self.to_dict()})
        except ConnectionError:
            pass

        self._move_keys_from(successor, predecessor)
        self.stabilize()
        self.fix_fingers()
        self.joined = True

    def _move_keys_from(self, successor: NodeInfo, predecessor: NodeInfo) -> None:
        if self._same(successor):
            return
        try:
            response = self.send_request(successor.host, successor.port, {"type": "LIST_PRIMARY"})
        except ConnectionError:
            return
        for key, value in ((x["key"], x["value"]) for x in response.get("items", [])):
            key_id = self.storage.hash_key(key, self.m)
            if self._open_closed(key_id, predecessor.id, self.id):
                self.storage.put_primary(key, value)
                try:
                    self.send_request(successor.host, successor.port, {"type": "DELETE_LOCAL", "key": key})
                except ConnectionError:
                    pass
                self._replicate(key, value)

    # ------------------------------ routing ---------------------------------

    def find_successor(self, key_id: int, include_hops: bool = False):
        result = self._lookup(key_id % self.ring_size, 0, set(), self.LOOKUP_LIMIT)
        return result if include_hops else result.node

    def _lookup(self, key_id: int, hops: int, seen: set[int], remaining: int) -> LookupResult:
        if remaining <= 0 or self.id in seen:
            raise RuntimeError("Chord lookup could not complete")
        seen = set(seen)
        seen.add(self.id)

        successor = self._successor_info()
        if self._same(successor) or self._open_closed(key_id, self.id, successor.id):
            return LookupResult(successor, hops + (0 if self._same(successor) else 1))

        for candidate in self._routing_candidates(key_id):
            if candidate.id in seen or self._same(candidate) or not self._open_open(candidate.id, self.id, key_id):
                continue
            try:
                response = self.send_request(candidate.host, candidate.port, {
                    "type": "FIND_SUCCESSOR",
                    "key_id": key_id,
                    "include_hops": True,
                    "hop_limit": remaining - 1,
                    "visited": list(seen),
                })
                if response.get("type") == "FIND_SUCCESSOR_RESPONSE":
                    result = NodeInfo.from_dict(response["node"])
                    self._remember(result)
                    return LookupResult(result, hops + int(response.get("hops", 0)) + 1)
            except (ConnectionError, RuntimeError):
                self._forget(candidate.id)

        raise RuntimeError(f"unable to route key {key_id} from node {self.id}")

    def _routing_candidates(self, key_id: int) -> list[NodeInfo]:
        with self._lock:
            candidates = list(reversed(self.finger_table)) + [self._successor] + list(self.known_peers.values())
        result, seen = [], set()
        for candidate in candidates:
            token = (candidate.id, candidate.port)
            if token not in seen:
                seen.add(token)
                result.append(candidate)
        result.sort(key=lambda n: (n.id - self.id) % self.ring_size, reverse=True)
        return result

    def find_predecessor(self, key_id: int) -> NodeInfo:
        successor = self.find_successor(key_id)
        if self._same(successor):
            return self.predecessor.info if isinstance(self.predecessor, Node) else self.predecessor
        try:
            response = self.send_request(successor.host, successor.port, {"type": "GET_PREDECESSOR"})
            return NodeInfo.from_dict(response["node"])
        except ConnectionError:
            return self.predecessor.info if isinstance(self.predecessor, Node) else self.predecessor

    def closest_preceding_node(self, key_id: int) -> NodeInfo:
        key_id %= self.ring_size
        if key_id == self.id:
            return self.info
        with self._lock:
            fingers = list(reversed(self.finger_table))
        for finger in fingers:
            if finger.id != self.id and self._open_open(finger.id, self.id, key_id):
                return finger
        return self.info

    # ------------------------------- upkeep ---------------------------------

    def stabilize(self) -> None:
        successor = self._successor_info()
        if self._same(successor):
            self.finger_table[0] = self.info
            self.successor_list = [self.info]
            return

        try:
            response = self.send_request(successor.host, successor.port, {"type": "GET_PREDECESSOR"})
            candidate_data = response.get("node")
            candidate = NodeInfo.from_dict(candidate_data) if candidate_data else successor
            self._remember(candidate)
        except ConnectionError:
            successor = self._find_live_successor()
            self.successor = successor
            self.finger_table[0] = successor
            self._refresh_successors()
            return

        if not self._same(candidate) and self._open_open(candidate.id, self.id, successor.id):
            self.successor = candidate
            successor = candidate
        self.finger_table[0] = successor
        try:
            self.send_request(successor.host, successor.port, {"type": "NOTIFY", "node": self.to_dict()})
        except ConnectionError:
            pass
        self._refresh_successors()

    def fix_fingers(self) -> None:
        self.finger_table[0] = self._successor_info()
        if self.m == 1:
            return
        i = self._finger_index
        self._finger_index = 1 if i + 1 >= self.m else i + 1
        try:
            self.finger_table[i] = self.find_successor((self.id + 2**i) % self.ring_size)
            self._remember(self.finger_table[i])
        except (ConnectionError, RuntimeError):
            self.finger_table[i] = self._successor_info()

    def check_predecessor(self) -> None:
        pred = self._predecessor
        if not pred or self._same(pred):
            return
        try:
            self.send_request(pred.host, pred.port, {"type": "PING"})
        except ConnectionError:
            self.predecessor = None

    def _refresh_successors(self) -> None:
        result: list[NodeInfo] = []
        current = self._successor_info()
        seen = {self.id}
        target = max(3, int(self.replication_factor))
        while current and current.id not in seen and len(result) < target:
            try:
                self.send_request(current.host, current.port, {"type": "PING"})
            except ConnectionError:
                break
            result.append(current)
            seen.add(current.id)
            try:
                response = self.send_request(current.host, current.port, {"type": "GET_SUCCESSOR"})
                current = NodeInfo.from_dict(response["node"])
                self._remember(current)
            except ConnectionError:
                break
        self.successor_list = result or [self.info]

    def _find_live_successor(self) -> NodeInfo:
        with self._lock:
            candidates = self.successor_list + list(reversed(self.finger_table)) + list(self.known_peers.values())
        unique = {c.id: c for c in candidates if c.id != self.id}
        for candidate in sorted(unique.values(), key=lambda n: (n.id - self.id) % self.ring_size):
            try:
                self.send_request(candidate.host, candidate.port, {"type": "PING"})
                return candidate
            except ConnectionError:
                self._forget(candidate.id)
        return self.info

    def _forget(self, node_id: int) -> None:
        with self._lock:
            self.known_peers.pop(node_id, None)
            self.finger_table = [self.info if f.id == node_id else f for f in self.finger_table]
            if self._successor.id == node_id:
                self._successor = self.info

    # ------------------------------ storage ---------------------------------

    def _store_primary(self, key: str, value) -> dict[str, Any]:
        # Refresh our immediate successor before choosing replica locations.
        # This keeps replicas aligned with the current Chord ring after joins.
        if self.joined:
            try:
                self.stabilize()
            except Exception:
                pass
        self.storage.put_primary(key, value)
        self._replicate(key, value)
        return self._put_response(key)

    def _put_response(self, key: str) -> dict[str, Any]:
        return {"type": "PUT_RESPONSE", "success": True, "key": key,
                "key_id": self.storage.hash_key(key, self.m), "node_id": self.id}

    def _successor_chain(self, owner: NodeInfo, count: int) -> list[NodeInfo]:
        """Return up to ``count`` live successors of ``owner``."""
        result: list[NodeInfo] = []
        seen = {owner.id}
        current = owner

        for _ in range(count):
            try:
                if self._same(current):
                    nxt = self._successor_info()
                else:
                    response = self.send_request(current.host, current.port, {"type": "GET_SUCCESSOR"})
                    nxt = NodeInfo.from_dict(response["node"])
            except (ConnectionError, KeyError, ValueError):
                nxt = self._find_next_from(current)
                if nxt is None:
                    break

            if nxt.id in seen:
                break

            try:
                self.send_request(nxt.host, nxt.port, {"type": "PING"})
            except ConnectionError:
                fallback = self._find_next_from(nxt)
                if fallback is None or fallback.id in seen:
                    break
                nxt = fallback

            result.append(nxt)
            seen.add(nxt.id)
            self._remember(nxt)
            current = nxt

        return result

    def _find_next_from(self, dead: NodeInfo) -> NodeInfo | None:
        with self._lock:
            peers = list(self.known_peers.values()) + list(self.finger_table)
        peers = [p for p in peers if p.id not in {self.id, dead.id}]
        peers.sort(key=lambda n: (n.id - dead.id) % self.ring_size)
        for peer in peers:
            try:
                self.send_request(peer.host, peer.port, {"type": "PING"})
                return peer
            except ConnectionError:
                self._forget(peer.id)
        return None

    def _replica_nodes(self, owner: NodeInfo) -> list[NodeInfo]:
        """Find the next live nodes clockwise from the primary owner."""
        needed = max(0, int(self.replication_factor) - 1)
        if needed == 0:
            return []

        with self._lock:
            pool = list(self.known_peers.values())
            pool += list(self.successor_list) + list(self.finger_table)

        unique = {node.id: node for node in pool if node.id != owner.id}
        candidates = sorted(unique.values(), key=lambda n: (n.id - owner.id) % self.ring_size)
        result: list[NodeInfo] = []

        for candidate in candidates:
            try:
                self.send_request(candidate.host, candidate.port, {"type": "PING"})
                result.append(candidate)
                if len(result) == needed:
                    return result
            except ConnectionError:
                self._forget(candidate.id)

        # If our local peer cache is incomplete, walk the owner's successor chain.
        for candidate in self._successor_chain(owner, needed - len(result)):
            if candidate.id != owner.id and all(candidate.id != x.id for x in result):
                result.append(candidate)
                if len(result) == needed:
                    break
        return result

    def _replicate(self, key: str, value) -> None:
        owner = self.info
        for replica in self._replica_nodes(owner):
            try:
                self.send_request(replica.host, replica.port, {
                    "type": "REPLICATE",
                    "key": key,
                    "value": value,
                })
            except ConnectionError:
                pass

    def _route_put(self, key: str, value) -> dict[str, Any]:
        owner = self.find_successor(self.storage.hash_key(key, self.m))
        if self._same(owner):
            return self._store_primary(key, value)
        return self.send_request(owner.host, owner.port, {"type": "STORE_PRIMARY", "key": key, "value": value})

    def _route_get(self, key: str) -> dict[str, Any]:
        owner = self.find_successor(self.storage.hash_key(key, self.m))
        if self._same(owner):
            value = self.storage.get(key)
            if value is None:
                for replica in self._replica_nodes(owner):
                    try:
                        response = self.send_request(replica.host, replica.port, {"type": "GET_REPLICA", "key": key})
                        if response.get("success"):
                            value = response["value"]
                            break
                    except ConnectionError:
                        pass
            return {"type": "GET_RESPONSE", "success": value is not None, "key": key, "value": value, "node_id": self.id}

        try:
            response = self.send_request(owner.host, owner.port, {"type": "GET_PRIMARY", "key": key})
            if response.get("success"):
                return response
        except ConnectionError:
            pass
        for replica in self._replica_nodes(owner):
            try:
                response = self.send_request(replica.host, replica.port, {"type": "GET_REPLICA", "key": key})
                if response.get("success"):
                    response["node_id"] = replica.id
                    return response
            except ConnectionError:
                pass
        return {"type": "GET_RESPONSE", "success": False, "key": key, "value": None, "node_id": owner.id}

    def _route_delete(self, key: str) -> dict[str, Any]:
        owner = self.find_successor(self.storage.hash_key(key, self.m))
        if not self._same(owner):
            return self.send_request(owner.host, owner.port, {"type": "DELETE_PRIMARY", "key": key})
        deleted = self.storage.delete(key)
        for replica in self._replica_nodes(owner):
            try:
                self.send_request(replica.host, replica.port, {"type": "DELETE_REPLICA", "key": key})
            except ConnectionError:
                pass
        return {"type": "DELETE_RESPONSE", "success": deleted, "key": key, "node_id": self.id}

    # ---------------------------- message API -------------------------------

    def handle_message(self, message: dict[str, Any]) -> dict[str, Any]:
        kind = message.get("type")

        if kind == "PING":
            return {"type": "PONG", "node_id": self.id}

        if kind == "FIND_SUCCESSOR":
            if message.get("key_id") is None:
                return {"type": "ERROR", "message": "Missing key_id"}
            try:
                visited = {int(x) for x in message.get("visited", [])}
                result = self._lookup(int(message["key_id"]) % self.ring_size, 0, visited,
                                      int(message.get("hop_limit", self.LOOKUP_LIMIT)))
                out = {"type": "FIND_SUCCESSOR_RESPONSE", "node": result.node.to_dict()}
                if message.get("include_hops"):
                    out["hops"] = result.hops
                return out
            except Exception as exc:
                return {"type": "ERROR", "message": str(exc)}

        if kind == "GET_SUCCESSOR":
            return {"type": "SUCCESSOR_RESPONSE", "node": self._successor_info().to_dict()}

        if kind == "GET_PREDECESSOR":
            pred = self._predecessor
            return {"type": "PREDECESSOR_RESPONSE", "node": pred.to_dict() if pred else None}

        if kind in {"NOTIFY", "SET_SUCCESSOR"}:
            candidate = NodeInfo.from_dict(message["node"])
            self._remember(candidate)
            if kind == "SET_SUCCESSOR":
                self.successor = candidate
                self.finger_table[0] = candidate
            else:
                current = self._predecessor
                if current is None or self._same(current) or self._open_open(candidate.id, current.id, self.id):
                    self.predecessor = candidate
            return {"type": "NOTIFY_RESPONSE" if kind == "NOTIFY" else "OK", "node_id": self.id}

        if kind == "STORE_PRIMARY":
            if message.get("key") is None or "value" not in message:
                return {"type": "ERROR", "message": "Missing key or value"}
            return self._store_primary(message["key"], message["value"])

        if kind == "REPLICATE":
            if message.get("key") is None or "value" not in message:
                return {"type": "ERROR", "message": "Missing key or value"}
            self.storage.put_replica(message["key"], message["value"])
            return {"type": "OK", "node_id": self.id}

        if kind in {"GET_PRIMARY", "GET_REPLICA"}:
            key = message.get("key")
            if key is None:
                return {"type": "ERROR", "message": "Missing key"}
            value = self.storage.get(key)
            return {"type": "GET_RESPONSE", "success": value is not None,
                    "key": key, "value": value, "node_id": self.id}

        if kind == "DELETE_PRIMARY":
            key = message.get("key")
            if key is None:
                return {"type": "ERROR", "message": "Missing key"}
            deleted = self.storage.delete(key)
            for replica in self._replica_nodes(self.info):
                try:
                    self.send_request(replica.host, replica.port, {"type": "DELETE_REPLICA", "key": key})
                except ConnectionError:
                    pass
            return {"type": "DELETE_RESPONSE", "success": deleted, "key": key, "node_id": self.id}

        if kind == "DELETE_REPLICA":
            key = message.get("key")
            if key is None:
                return {"type": "ERROR", "message": "Missing key"}
            return {"type": "OK", "success": self.storage.delete(key), "node_id": self.id}

        if kind == "DELETE_LOCAL":
            key = message.get("key")
            if key is None:
                return {"type": "ERROR", "message": "Missing key"}
            return {"type": "OK", "success": self.storage.delete(key), "node_id": self.id}

        if kind == "LIST_PRIMARY":
            return {"type": "PRIMARY_ITEMS_RESPONSE",
                    "items": [{"key": k, "value": v} for k, v in self.storage.primary_items()],
                    "node_id": self.id}

        if kind == "PUT":
            key = message.get("key")
            if key is None:
                return {"type": "ERROR", "message": "Missing key"}
            if "value" not in message:
                return {"type": "ERROR", "message": "Missing value"}
            try:
                return self._route_put(key, message["value"])
            except Exception as exc:
                return {"type": "PUT_RESPONSE", "success": False, "key": key, "message": str(exc), "node_id": self.id}

        if kind == "GET":
            key = message.get("key")
            if key is None:
                return {"type": "ERROR", "message": "Missing key"}
            try:
                return self._route_get(key)
            except Exception:
                return {"type": "GET_RESPONSE", "success": False, "key": key, "value": None, "node_id": self.id}

        if kind == "DELETE":
            key = message.get("key")
            if key is None:
                return {"type": "ERROR", "message": "Missing key"}
            try:
                return self._route_delete(key)
            except Exception:
                return {"type": "DELETE_RESPONSE", "success": False, "key": key, "node_id": self.id}

        return {"type": "ERROR", "message": f"Unknown message type: {kind}"}

    # ---------------------------- convenience ------------------------------

    def send_ping(self, host: str, port: int):
        return self.send_request(host, port, {"type": "PING", "sender_id": self.id})

    def send_put(self, host: str, port: int, key: str, value):
        return self.send_request(host, port, {"type": "PUT", "key": key, "value": value})

    def send_get(self, host: str, port: int, key: str):
        return self.send_request(host, port, {"type": "GET", "key": key})

    def send_delete(self, host: str, port: int, key: str):
        return self.send_request(host, port, {"type": "DELETE", "key": key})


def main() -> None:
    parser = argparse.ArgumentParser(description="ChordFS node")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--m", type=int, default=8)
    parser.add_argument("--replication-factor", type=int, default=1)
    parser.add_argument("--join-host")
    parser.add_argument("--join-port", type=int)
    args = parser.parse_args()

    node = Node(args.host, args.port, args.m)
    node.replication_factor = max(1, args.replication_factor)
    thread = threading.Thread(target=node.start, daemon=True)
    thread.start()
    time.sleep(0.2)

    if args.join_host and args.join_port:
        node.join(args.join_host, args.join_port)
    else:
        node.join()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        node.stop()


if __name__ == "__main__":
    main()
