# ChordFS

A fault-tolerant peer-to-peer distributed key/value and file-storage system using the Chord Distributed Hash Table.

## Current implementation

- TCP communication, JSON protocol, framing, PING/PONG
- Hashing and persistent local storage
- Chord ring, membership, successor/predecessor, key migration
- Finger tables and logarithmic routing
- DHT-routed PUT/GET/DELETE and concurrent requests
- Configurable replication and replica failover
- Failure detection, ring repair, dead-node avoidance, restart/rejoin

## Run

```bash
python node.py --port 5001
python node.py --port 5002 --join-host 127.0.0.1 --join-port 5001
```

For replication:

```bash
python node.py --port 5001 --replication-factor 3
```

## Test

```bash
pip install -r requirements-dev.txt
pytest -q
```
