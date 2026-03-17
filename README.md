# Redis Protocol Implementation

A high-performance, asynchronous Redis server implementation in Rust, focused on low-latency and systems-level concurrency management.

## Core Implementation
- **Asynchronous Runtime:** Built on Tokio for high-performance, non-blocking I/O.
- **Protocol Parsing:** Implements the RESP (Redis Serialization Protocol) via a custom TCP frame-decoder designed for zero-copy parsing where possible.
- **Concurrency Management:** Utilizes a thread-safe global `Mutex<HashMap>` for data storage. High-concurrency stress tests reveal lock contention, identifying Database Sharding as a clear optimization path.
- **Performance:** Achieves ~180,000+ RPS (< 0.1ms p50 latency) under standard single-threaded workloads (`redis-benchmark`), and ~63,750 Ops/sec in multi-threaded stress tests (`memtier_benchmark`).

## Supported Commands
Handles core system operations, string manipulation, and list management pipelines:

| Command | Category |
| :--- | :--- |
| `PING` | System |
| `ECHO` | System |
| `TYPE` | System |
| `SET` | String |
| `GET` | String |
| `RPUSH` | List |
| `LPUSH` | List |
| `LRANGE` | List |
| `LLEN` | List |
| `LPOP` | List |
| `BLPOP` | List & Blocking |

## Usage
Start the server on port 6379 (build in release mode for maximum performance)
```bash
cargo run --release
```
Connect using the standard Redis client
```bash
redis-cli -p 6379
```