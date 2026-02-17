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
- **Built-in replication** — page-level dirty tracking with batched sync and automatic retry.
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
| Open / close | `albedo_open`, `albedo_close` | Pass `":memory:"` for an in-memory bucket |
| Insert | `albedo_insert` | Accepts a raw BSON document buffer |
| Query | `albedo_list` → `albedo_data` | `albedo_data` returns `ALBEDO_OK` with a document pointer, `ALBEDO_EOS` when done |
| Delete | `albedo_delete` | Tombstones matching docs; triggers auto-vacuum when deleted > live |
| Update | `albedo_transform` → `albedo_transform_data` / `albedo_transform_apply` | Iterate matches and apply per-document transforms |
| Indexes | `albedo_ensure_index`, `albedo_drop_index`, `albedo_list_indexes` | B⁺-tree indexes on arbitrary field paths |
| Maintenance | `albedo_vacuum`, `albedo_flush` | Compact the file or force-sync to disk |
| Replication | `albedo_set_replication_callback`, `albedo_apply_batch` | Page-level batched sync — see [REPLICATION.md](REPLICATION.md) |

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

