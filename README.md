<p align="center">
  <picture>
    <img alt="Albedo" src="https://img.shields.io/badge/Albedo-Document%20Store-blue?style=for-the-badge" height="36"/>
  </picture>
</p>

<p align="center">
  <a href="https://github.com/klirix/albedo/blob/main/license"><img src="https://img.shields.io/badge/license-MIT-green.svg" alt="MIT License"/></a>
</p>

---

## What is Albedo?

Albedo is an embedded document database. It stores BSON documents in a compact
page-based file format, indexes them with B⁺-trees, and exposes a portable
C ABI so virtually any language can use it as a library — no server process, no
network round-trips.

**Key features:**

- **Single-file storage** — one `.bucket` file holds documents, indexes, and metadata.
- **BSON native** — documents are stored and queried in BSON; no intermediate format.
- **B⁺-tree indexing** — create indexes on any field path, including nested and array fields.
- **Vector search filters** — query `f32` embedding payloads with cosine, dot-product, or euclidean thresholds.
- **Write-Ahead Log (WAL)** — enabled by default on Linux/macOS; provides crash recovery, MVCC reads, and cross-process live-tail without blocking writers.
- **Built-in replication** — page-level dirty tracking with batched sync and automatic retry.
- **Tunable write durability** — choose between per-write fsync (`.all`), periodic fsync (`.periodic(N)`), or fully manual (`.manual`) to trade safety for throughput.
- **Zero external dependencies** — the core is pure Zig; bindings are thin wrappers around the C ABI.
- **Runs everywhere** — the storage layer only needs basic file-handle read/write operations, so it cross-compiles cleanly for Linux, macOS, Windows, iOS, Android, and WASM.

---

## Install the C library via Homebrew

```sh
brew install klirix/tap/albedo
```

This gives you `libalbedo` (shared + static) and the C header ready to link
from any language.

---

## Language bindings

Albedo is designed to be consumed from many runtimes. Pick the one that fits:

| Language / Runtime | Package | Description |
|--------------------|---------|-------------|
| **Node / Bun** | [albedo-node](https://github.com/klirix/albedo-node) | N-API native addon — works with Node, Bun, and any N-API host |
| **JavaScript / WASM** | [albedo-wasm](https://github.com/klirix/albedo-wasm) | WebAssembly build for browsers and edge runtimes |
| **Dart / Flutter** | [albedo_flutter](https://github.com/klirix/albedo_flutter) | FFI plugin for Flutter & standalone Dart apps |
| **Crystal** | [albedo_cr](https://github.com/klirix/albedo_cr) | Crystal shard wrapping the C library |
| **C / C++** | [include/albedo.h](include/albedo.h) | Use the header directly — link against `libalbedo` |

---

## Quick start (C)

```c
#include "albedo.h"

albedo_bucket *db;
albedo_open("my.bucket", &db);

// Insert a BSON document (bytes built however you like)
albedo_insert(db, bson_buf);

// Query & iterate
albedo_list_handle *it;
albedo_list(db, query_buf, &it);
uint8_t *doc;
while (albedo_data(it, &doc) != ALBEDO_EOS) {
    // …use doc…
}
albedo_close_iterator(it);
albedo_close(db);
```

---

## Core operations

| Operation | Function | Notes |
|-----------|----------|-------|
| Open / close | `albedo_open`, `albedo_close` | Pass `":memory:"` for an in-memory bucket; WAL is enabled by default on POSIX |
| Insert | `albedo_insert` | Accepts a raw BSON document buffer |
| Query | `albedo_list` → `albedo_data` | `albedo_data` returns `ALBEDO_OK` with a document pointer, `ALBEDO_EOS` when done |
| Delete | `albedo_delete` | Tombstones matching docs; triggers auto-vacuum when deleted > live |
| Update | `albedo_transform` → `albedo_transform_data` / `albedo_transform_apply` | Iterate matches and apply per-document transforms |
| Indexes | `albedo_ensure_index`, `albedo_drop_index`, `albedo_list_indexes` | B⁺-tree indexes on arbitrary field paths |
| Maintenance | `albedo_vacuum`, `albedo_flush` | Compact the file or force-sync to disk (`flush` fsyncs the WAL in WAL mode) |
| Replication | `albedo_set_replication_callback`, `albedo_apply_batch` | Page-level batched sync — see [REPLICATION.md](REPLICATION.md) |

See [include/albedo.h](include/albedo.h) for the full C API surface.

---

## Vector Search

Albedo supports `$vectorSearch` as a query filter operator. It compares a
stored vector field against a query vector and applies a threshold condition.

Supported algorithms:

- `cosine`
- `dot`
- `euclidean`

Supported threshold operators inside `operand`:

- `$gte` / `gte`
- `$lte` / `lte`
- `$gt` / `gt`
- `$lt` / `lt`

Query shape:

```bson
{
  "query": {
    "embedding": {
      "$vectorSearch": {
        "algo": "cosine",
        "vector": <BSON binary of f32[]>,
        "operand": { "$gte": 0.85 }
      }
    }
  }
}
```

Important validation rules:

- Payload alignment must be valid for `f32`.
- Any non-`f32` vector format is rejected.
- Invalid vector payloads are treated as query errors and are not computed as best-effort fallbacks.

---

## Building from source

Requires [Zig](https://ziglang.org) (0.15.1).

```sh
# Shared library (default)
zig build

# Static library
zig build -Dstatic=true

# Run the test suite
zig build test
```

Build artifacts land in `zig-out/`. See [build.md](build.md) for
platform-specific notes and Android cross-compilation.

---

## Write-Ahead Log (WAL)

On Linux and macOS, Albedo opens databases in WAL mode by default. Every page
write is appended to a `<name>-wal` file instead of modifying the main DB file
directly. A memory-mapped shared-memory index (`<name>-wal-shm`) lets multiple
processes read the latest page versions without blocking each other.

**What WAL gives you:**

- **Crash recovery** — uncommitted data in the WAL is replayed on the next open.
- **MVCC reads** — readers always see a consistent snapshot; writers never block readers.
- **Live-tail / document streaming** — a reader can keep a `listIterate` iterator open and call `next()` in a poll loop. When the iterator is exhausted it automatically refreshes from the WAL and picks up documents added by another connection.
- **Throughput** — page writes bypass `fsync` by default (`.manual` write-durability mode), matching SQLite’s `synchronous=NORMAL` in WAL mode.

**Write-durability modes** (set via `OpenBucketOptions.write_durability` in Zig):

| Mode | Behaviour |
|------|-----------|
| `.all` | `fsync` after every page write — safest, slowest |
| `.{ .periodic = N }` | `fsync` every N page writes (default: 100) |
| `.manual` | Never auto-`fsync`; call `albedo_flush` / `flush()` when you need a durability guarantee |

The WAL is checkpointed (applied to the main DB file and deleted) automatically
when the last connection closes. While any connection is open the WAL is kept
alive so other readers can continue using it.

---

## Streaming Queries

`albedo_list` can be used as a document stream even without cursors. Open a
list iterator, call `albedo_data` / `next()`, and keep polling after `EOS` if
you want live-tail behavior. In WAL mode the iterator can pick up documents
written later by another connection.

Basic C flow:

```c
albedo_list_handle *it;
albedo_list(db, query_buf, &it);

uint8_t *doc;
while (albedo_data(it, &doc) == ALBEDO_OK) {
  // consume doc
}

// Later, poll again on the same iterator if you want to keep streaming.
while (albedo_data(it, &doc) == ALBEDO_OK) {
  // consume newly visible docs
}
```

Streaming queries are useful for:

- Full-scan streams over the whole bucket
- Range-index streams such as `{ "query": { "age": { "$gte": 30 } } }`
- Long-lived readers that want to keep an iterator open and observe new writes

Current streaming limitations:

- Queries with `sort` are materialized eagerly and are not stream-shaped
- `sector` is pagination, not streaming state
- Point-strategy index scans such as `$in` are supported as normal queries, but
  not as resumable cursor streams

## Streaming Cursors

A cursor is an exported snapshot of stream progress. Use it when you want to
close an iterator, hand the state to a client, and reopen the same streaming
query later without replaying already delivered documents.

Cursor shape:

```bson
{
  "query": { ... },
  "cursor": {
    "version": 1,
    "mode": "full_scan" | "index_range",
    "indexPath": "field.path",
    "anchor": {
      "docId": ObjectId("..."),
      "_id": <BSON value>,
      "pageId": 42,
      "offset": 128
    }
  }
}
```

C API flow:

```c
albedo_list_handle *it;
albedo_list(db, query_buf, &it);

uint8_t *doc;
albedo_data(it, &doc);

uint8_t *cursor_buf;
albedo_list_cursor_export(it, &cursor_buf);
albedo_close_iterator(it);

// Build a new query buffer with {"cursor": <cursor_buf>}
albedo_list(db, resumed_query_buf, &it);
```

Cursor-specific limitations in v1:

- No `sort` with `cursor`
- No `sector` with `cursor`
- No point-strategy index cursors such as `$in`
- Cursor iterators are not thread-safe
- Best-effort continuation only; this is not snapshot pagination

Invalidation and resume errors:

- A cursor is tied to the current document layout and stream anchor
- After `vacuum()`, previously exported cursors are not accepted
- If the anchor can no longer be found when reopening, resume fails with
  `InvalidCursor` in Zig and `ALBEDO_INVALID_CURSOR` in the C API

---

## Replication

Albedo ships with a page-level replication system that batches dirty pages and
pushes them to replicas via a simple callback. See
[REPLICATION.md](REPLICATION.md) for the full protocol description.

---

## Project status

Albedo is pre 1.0. The on-disk format and public APIs may
change between versions. Contributions and bug reports are welcome.

---

## License

Released under the [MIT License](license). © 2025 Askhat Saiapov.
