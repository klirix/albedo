# Getting started

[Documentation index](README.md) ·
[Building](building.md) ·
[Configuration](configuration.md) ·
[Query language](query-language.md)

This guide covers installation, language bindings, the C quick start, and the
main public operations. See [building.md](building.md) for source-build details.

## Install the C library via Homebrew

```sh
brew install klirix/tap/albedo
```

This installs `libalbedo` as shared and static libraries, together with the C
header.

## Language bindings

| Language / runtime | Package | Description |
|--------------------|---------|-------------|
| Node / Bun | [albedo-node](https://github.com/klirix/albedo-node) | N-API native addon for Node, Bun, and other N-API hosts |
| JavaScript / WASM | [albedo-wasm](https://github.com/klirix/albedo-wasm) | WebAssembly build for browsers and edge runtimes |
| Dart / Flutter | [albedo_flutter](https://github.com/klirix/albedo_flutter) | FFI plugin for Flutter and standalone Dart |
| Crystal | [albedo_cr](https://github.com/klirix/albedo_cr) | Crystal shard wrapping the C library |
| C / C++ | [include/albedo.h](../include/albedo.h) | Link directly against `libalbedo` |

## Quick start with C

```c
#include "albedo.h"

albedo_bucket_handle *db;
albedo_open("my.bucket", &db);

// Insert a BSON document built with your BSON library of choice.
albedo_insert(db, bson_buf);

// Query and iterate.
albedo_list_handle *it;
albedo_list(db, query_buf, &it);

uint8_t *doc;
while (albedo_data(it, &doc) != ALBEDO_EOS) {
    // Use doc before advancing the iterator.
}

albedo_close_iterator(it);
albedo_close(db);
```

Query buffers use the format documented in the
[query-language guide](query-language.md).

## Core operations

| Operation | Function | Notes |
|-----------|----------|-------|
| Open / close | `albedo_open`, `albedo_close` | Pass `":memory:"` for an in-memory bucket; WAL is enabled by default on POSIX |
| Insert | `albedo_insert` | Accepts a raw BSON document buffer |
| Query | `albedo_list` → `albedo_data` | Returns `ALBEDO_OK` with a document or `ALBEDO_EOS` when no document is currently available |
| Delete | `albedo_delete` | Tombstones matching documents and may trigger auto-vacuum |
| Update | `albedo_transform` → `albedo_transform_data` / `albedo_transform_apply` | Iterates matches and accepts complete replacement documents |
| Indexes | `albedo_ensure_index`, `albedo_drop_index`, `albedo_list_indexes` | B⁺-tree indexes on arbitrary field paths |
| Transactions | `albedo_transaction_begin` and transaction mutation functions | Groups mutations under explicit commit or rollback |
| Maintenance | `albedo_checkpoint`, `albedo_vacuum`, `albedo_flush` | Checkpoint WAL state, compact storage, or force-sync |
| Replication | `albedo_replication_cursor`, `albedo_replication_read`, `albedo_replication_apply` | Cursor-based committed WAL replication |
| Subscriptions | `albedo_subscribe`, `albedo_subscribe_poll`, `albedo_subscribe_close` | Real-time change streams; requires WAL mode |

See [include/albedo.h](../include/albedo.h) for the complete C ABI.

## Where to go next

- Configure file-backed buckets and durability:
  [configuration and durability](configuration.md)
- Write filters and result shaping:
  [query language](query-language.md)
- Apply expression-based updates from Zig:
  [update expressions](update-expressions.md)
- Consume long-lived result streams:
  [streaming and cursors](streaming.md)
- Observe mutations as events:
  [subscriptions](subscriptions.md)
