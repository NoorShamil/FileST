from __future__ import annotations

import hashlib
import json
import threading
from pathlib import Path
from typing import Iterator


class Storage:

    def __init__(self, node_id: int, base_directory: str = "data"):
        self.node_id = node_id
        self.directory = Path(base_directory) / f"node_{node_id}"
        self.directory.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()

    @staticmethod
    def hash_key(key: str, m: int = 8) -> int:
        if m <= 0:
            raise ValueError("m must be positive")
        digest = hashlib.sha256(key.encode("utf-8")).digest()
        hash_value = int.from_bytes(digest, byteorder="big")
        return hash_value % (2**m)

    def get_path(self, key: str) -> Path:
        key_hash = hashlib.sha256(key.encode("utf-8")).hexdigest()
        return self.directory / key_hash

    def _write_record(self, key: str, value, role: str) -> None:
        path = self.get_path(key)
        record = {"key": key, "value": value, "role": role}
        with path.open("w", encoding="utf-8") as file:
            json.dump(record, file, ensure_ascii=False)

    def put(self, key: str, value) -> None:
        """Store a primary record. Kept compatible with Week 2."""
        with self._lock:
            self._write_record(key, value, "primary")

    def put_primary(self, key: str, value) -> None:
        with self._lock:
            self._write_record(key, value, "primary")

    def put_replica(self, key: str, value) -> None:
        with self._lock:
            self._write_record(key, value, "replica")

    def get(self, key: str):
        with self._lock:
            path = self.get_path(key)
            if not path.exists():
                return None
            with path.open("r", encoding="utf-8") as file:
                record = json.load(file)
            return record["value"]

    def get_record(self, key: str):
        with self._lock:
            path = self.get_path(key)
            if not path.exists():
                return None
            with path.open("r", encoding="utf-8") as file:
                return json.load(file)

    def get_role(self, key: str) -> str | None:
        record = self.get_record(key)
        if record is None:
            return None
        return record.get("role", "primary")

    def delete(self, key: str) -> bool:
        with self._lock:
            path = self.get_path(key)
            if not path.exists():
                return False
            path.unlink()
            return True

    def exists(self, key: str) -> bool:
        return self.get_path(key).exists()

    def items(self, role: str | None = None) -> Iterator[tuple[str, object]]:
        
        with self._lock:
            paths = list(self.directory.iterdir())
            for path in paths:
                if not path.is_file():
                    continue
                try:
                    with path.open("r", encoding="utf-8") as file:
                        record = json.load(file)
                except (OSError, json.JSONDecodeError, KeyError):
                    continue
                record_role = record.get("role", "primary")
                if role is not None and record_role != role:
                    continue
                yield record["key"], record["value"]

    def primary_items(self) -> list[tuple[str, object]]:
        return list(self.items(role="primary"))

    def replica_items(self) -> list[tuple[str, object]]:
        return list(self.items(role="replica"))
