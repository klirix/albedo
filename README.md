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

Albedo is an embedded BSON document database. It stores documents in a compact
page-based file, indexes them with B⁺-trees, and exposes both a Zig API and a
portable C ABI. There is no server process or network round-trip.

Key properties:

- **Single-file storage** for documents, indexes, and metadata
- **BSON-native** storage and queries
- **B⁺-tree indexes** on nested and array field paths
- **Write-ahead logging** with crash recovery and MVCC reads
- **Cross-process streaming** and real-time subscriptions
- **WAL-native replication** with caller-owned transport
- **Query-driven update expressions** in the Zig API
- **Portable core** for Linux, macOS, Windows, iOS, Android, and WASM

## Install

Install the C library and header with Homebrew:

```sh
brew install klirix/tap/albedo
```

To build from source:

```sh
zig build
zig build test
```

See [building Albedo](docs/building.md) for build targets and artifacts.

## Quick start with C

```c
#include "albedo.h"

albedo_bucket_handle *db;
albedo_open("my.bucket", &db);

albedo_insert(db, bson_buf);

albedo_list_handle *it;
albedo_list(db, query_buf, &it);

uint8_t *doc;
while (albedo_data(it, &doc) == ALBEDO_OK) {
    // Consume the BSON document.
}

albedo_close_iterator(it);
albedo_close(db);
```

See [getting started](docs/getting-started.md) for the core operations and
the [C header](include/albedo.h) for the complete ABI.

## Documentation

The documentation is split into task-focused guides under [`docs/`](docs/README.md):

| Guide | Covers |
|-------|--------|
| [Getting started](docs/getting-started.md) | Installation, bindings, C quick start, and core operations |
| [Building](docs/building.md) | Build commands, targets, and artifacts |
| [Configuration and durability](docs/configuration.md) | Bucket options, WAL, caching, checkpoints, and durability |
| [Query language](docs/query-language.md) | Filters, logical operators, sorting, pagination, projections, and planning |
| [Update expressions](docs/update-expressions.md) | Query-driven Zig updates, stages, pipelines, and expressions |
| [Streaming and cursors](docs/streaming.md) | Long-lived iterators and resumable query state |
| [Subscriptions](docs/subscriptions.md) | Real-time insert, update, and delete events |
| [Replication](docs/replication.md) | Committed WAL batches, cursors, apply rules, and resnapshotting |

For contributors and coding agents, [AGENTS.md](AGENTS.md) describes the
on-disk format, storage machinery, concurrency model, and core invariants.

## Language bindings

| Language / runtime | Package |
|--------------------|---------|
| Node / Bun | [albedo-node](https://github.com/klirix/albedo-node) |
| JavaScript / WASM | [albedo-wasm](https://github.com/klirix/albedo-wasm) |
| Dart / Flutter | [albedo_flutter](https://github.com/klirix/albedo_flutter) |
| Crystal | [albedo_cr](https://github.com/klirix/albedo_cr) |
| C / C++ | [include/albedo.h](include/albedo.h) |

## Project status

Albedo is pre-1.0. The on-disk format and public APIs may change between
versions. Contributions and bug reports are welcome.

## License

Released under the [MIT License](license). © 2025 Askhat Saiapov.
