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

**Key properties:**

- **Single-file storage** — one `.bucket` file holds documents, indexes, and metadata.
- **BSON native** — documents are stored and queried in BSON; no intermediate format.
- **B⁺-tree indexing** — create indexes on any field path, including nested and array fields.
- **Write-Ahead Log (WAL)** — enabled by default on Linux/macOS; provides crash recovery, MVCC reads, and cross-process live-tail without blocking writers.
- **Built-in replication** — WAL-native cursor APIs stream committed frame batches; transport and retry are owned by the caller.
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

albedo_bucket_handle *db;
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
| Replication | `albedo_replication_cursor`, `albedo_replication_read`, `albedo_replication_apply`, `albedo_replication_cursor_close` | Cursor-based WAL replication — see [REPLICATION.md](REPLICATION.md) |
| Subscriptions | `albedo_subscribe`, `albedo_subscribe_poll`, `albedo_subscribe_close` | Real-time oplog change stream (insert / update / delete events) — requires WAL mode |

See [include/albedo.h](include/albedo.h) for the full C API surface.

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
- **Throughput** — page writes bypass `fsync` by default (`.manual` write-durability mode), matching SQLite's `synchronous=NORMAL` in WAL mode.

The WAL is checkpointed (applied to the main DB file and deleted) automatically
when the last connection closes. While any connection is open the WAL is kept
alive so other readers can continue using it.

---

## Init options

These options are passed via `OpenBucketOptions` in Zig (or as a BSON document
to `albedo_open_with_options` in C).

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `wal` | `bool` | `true` | Enable the Write-Ahead Log. Disable only for read-heavy single-process workloads that do not need crash recovery or MVCC. |
| `oplog_size` | `u32` | `4 MiB` | Size of the oplog circular ring buffer in bytes. `0` disables the oplog entirely (change-stream subscriptions will not work). Must match the value used when the SHM file was first created; a mismatch returns an error. Older SHM files (reserved bytes were zero) are seamlessly re-initialized. |
| `write_durability` | see below | `.{ .periodic = 100 }` | Controls when `fsync` is called. |
| `read_durability` | see below | `.shared` | Controls how page reads interact with the WAL. |
| `auto_vaccuum` | `bool` | `true` | Automatically compact the database when deleted pages exceed live pages. |
| `page_cache_capacity` | `usize` | `256` | Maximum number of pages held in the in-process LRU page cache. |
| `mode` | `ReadOnly` / `ReadWrite` | `ReadWrite` | Open the file read-only or read-write. |

**Write-durability modes** (`write_durability`):

| Mode | Behaviour |
|------|-----------|
| `.all` | `fsync` after every page write — safest, slowest |
| `.{ .periodic = N }` | `fsync` every N page writes (default: 100) |
| `.manual` | Never auto-`fsync`; call `albedo_flush` / `flush()` when you need a durability guarantee |

**Read-durability modes** (`read_durability`):

| Mode | Behaviour |
|------|-----------|
| `.shared` | Always consult the WAL before returning a cached page — safe for multi-process readers |
| `.process` | Trust the local in-process cache; fall back to the WAL only on a cache miss — best performance for single-process workloads |

---

## Streaming Queries

`albedo_list` can be used as a document stream even without cursors. Open a
list iterator, call `albedo_data` / `next()`, and keep polling after `EOS` if
you want live-tail behavior. In WAL mode the iterator can pick up documents
written later by another connection.

> **Real-time use case?** If you need low-latency notification of individual
> inserts, updates, and deletes — rather than a full re-scan — use the
> [Subscriptions](#subscriptions) API instead. Subscriptions read from the
> oplog ring and return change events immediately, with no page scanning.

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

## Subscriptions

Subscriptions give you a real-time change stream over a WAL-mode bucket.
Rather than re-scanning pages, a subscription reads from a circular oplog
ring buffer kept in the WAL shared-memory file. Each entry is a compact
operation envelope — insert, update, or delete — with the document embedded
inline when it fits within 1 KB.

**When to prefer subscriptions over streaming queries:**

- You need individual change notifications (insert / update / delete) rather than a document stream.
- You want to observe changes made by *any* writer, not just the local connection.
- Polling latency matters more than throughput; the oplog ring is read without any page I/O.

### C API

```c
// 1. Open a bucket in WAL mode (default on Linux/macOS).
albedo_bucket_handle *db;
albedo_open("my.bucket", &db);

// 2. Subscribe. Pass an optional BSON query to receive only matching events.
//    An empty document {} matches everything.
albedo_subscription_handle *sub;
albedo_subscribe(db, query_buf, &sub);

// 3. Poll in a loop. Each successful poll returns a BSON document
//    {batch: [{seqno, op, doc_id, ts, doc?}, ...]}.
//    The document is owned by the subscription and valid only until the
//    next poll or close call.
uint8_t *batch_doc;
albedo_result r = albedo_subscribe_poll(sub, &batch_doc, 64);
if (r == ALBEDO_HAS_DATA) {
    // batch_doc is a BSON document: {batch: [...events]}.
    // Parse it with your BSON library of choice.
    // Lifetime: valid until the next albedo_subscribe_poll() or
    //           albedo_subscribe_close() call.
} else if (r == ALBEDO_EOS) {
    // No new events; sleep briefly and poll again.
} else if (r == ALBEDO_OPLOG_GAP) {
    // The subscriber fell too far behind and the ring wrapped.
    // Close the subscription and re-subscribe to resume.
    albedo_subscribe_close(sub);
    albedo_subscribe(db, query_buf, &sub);
}

// 4. Check the latest committed seqno without polling.
uint64_t seqno = albedo_subscribe_seqno(sub);

// 5. Clean up.
albedo_subscribe_close(sub);
albedo_close(db);
```

### Change event fields

Each element of the `batch` array is a BSON document with these fields:

| Field | BSON type | Description |
|-------|-----------|-------------|
| `seqno` | int64 | Monotonically increasing oplog sequence number |
| `op` | string | `"insert"`, `"update"`, or `"delete"` |
| `doc_id` | objectId | Document identifier (same as the BSON `_id` bytes) |
| `ts` | int64 | Unix nanoseconds when the operation was written |
| `doc` | document | *(present on insert/update with inline payload ≤ 1 KB)* Full BSON document body |

### Overflow and gap handling

The oplog ring is by default 4 MB, configurable via `oplog_size` in init options. If a subscriber polls infrequently and the
writer is active, the ring may wrap before the subscriber reads all entries.
When this happens `albedo_subscribe_poll` returns `ALBEDO_OPLOG_GAP`. The
correct recovery is to close the subscription, optionally perform a one-time
full scan of the collection to rebuild local state, and then re-subscribe to
resume from the current tail.

### Filtering

Pass a BSON query document (the same format as `albedo_list`) to
`albedo_subscribe`. Only insert and update events whose inline document
matches the query are delivered; delete events without an inline document
always pass through.

---

## Replication

Albedo replication is built directly on committed WAL history. A primary
publishes an opaque replication cursor handle, `albedo_replication_read`
returns `ReplicationBatchHeader + raw WAL frames`, and
`albedo_replication_apply` appends that exact range into a replica WAL.

- `ALBEDO_HAS_DATA` means a batch was returned.
- `ALBEDO_EOS` means there are no newer committed frames at the cursor.
- `ALBEDO_REPLICATION_GAP` means the cursor is stale after a WAL generation
  reset and the replica must resnapshot.
- `albedo_replication_read` returns an owned buffer; release it with
  `albedo_free`.
- `albedo_replication_cursor` / `albedo_replication_apply` return opaque cursor
  handles; release them with `albedo_replication_cursor_close`.

If you want, you can tail the WAL directly SQLite-style because the payload is
literally raw WAL frames, but the replication API is the safer contract because
it only exposes committed frames and carries generation metadata for WAL reset
detection. See [REPLICATION.md](REPLICATION.md) for the full protocol.

---

## Project status

Albedo is pre 1.0. The on-disk format and public APIs may
change between versions. Contributions and bug reports are welcome.

---

## License

Released under the [MIT License](license). © 2025 Askhat Saiapov.
