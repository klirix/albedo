# Configuration and durability

[Documentation index](README.md) ·
[Getting started](getting-started.md) ·
[Streaming](streaming.md) ·
[Replication](replication.md)

This guide describes bucket-open options and the write-ahead log. For API
setup, see [getting started](getting-started.md).

## Write-ahead log

On Linux and macOS, Albedo opens databases in WAL mode by default. WAL mode
provides:

- crash recovery after an abrupt process exit
- MVCC reads
- cross-process live-tail queries
- committed history for replication
- the shared-memory oplog used by subscriptions

The main bucket file is not updated on every mutation. Committed page images
remain in `<bucket>-wal` until a checkpoint applies them to the bucket file.

Use `albedo_checkpoint` or `Bucket.checkpoint()` to apply the WAL and reset it.
A manual checkpoint is useful to:

- bound WAL growth
- leave a standalone bucket file in a clean state
- prepare for maintenance or transfer

`albedo_flush` or `Bucket.flush()` forces pending writes to the configured
durable storage boundary. A flush does not replace checkpointing.

## Bucket-open options

These options are passed through `OpenBucketOptions` in Zig or as a BSON
document to `albedo_open_with_options` in C.

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `wal` | `bool` | `true` | Enable WAL mode; forced off where WAL support is unavailable |
| `oplog_size` | `u32` | `4 MiB` | Shared-memory subscription ring size; `0` disables subscriptions |
| `write_durability` | see below | `.{ .periodic = 100 }` | Controls automatic syncing |
| `read_durability` | see below | `.shared` | Controls cross-process read visibility |
| `wal_auto_checkpoint` | `u64` | `1000` | Checkpoint after this many live WAL frames; `0` disables auto-checkpointing |
| `auto_vaccuum` | `bool` | `true` | Compact automatically when deleted documents outnumber live documents |
| `page_cache_capacity` | `usize` | engine default | Maximum cached pages |
| `mode` | `ReadOnly` / `ReadWrite` | `ReadWrite` | File-open mode |

The spelling `auto_vaccuum` is part of the current public option structure.

## Write durability

| Mode | Behavior |
|------|----------|
| `.all` | Sync every logical write; strongest durability and highest sync cost |
| `.{ .periodic = N }` | Sync every `N` writes; the default is `100` |
| `.manual` | Do not sync automatically; call `flush()` explicitly |

Periodic and manual modes still publish logical WAL commit boundaries so an
abrupt process exit can recover completed mutations from the operating
system's file cache. Only `.all` or an explicit `flush()` provides the
corresponding stable-storage guarantee at that point.

## Read durability

| Mode | Behavior |
|------|----------|
| `.shared` | Consult the WAL for the latest cross-process page versions; safe default |
| `.process` | Trust the local page cache and consult WAL on cache misses; faster for a single process |

Use `.shared` when multiple processes or connections may write the same
bucket. Use `.process` only when stale cross-process cache entries are an
acceptable tradeoff.

## Related guides

- [Streaming and cursors](streaming.md)
- [Subscriptions](subscriptions.md)
- [Replication](replication.md)
