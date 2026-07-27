# WAL-native replication

[Documentation index](README.md) ·
[Configuration](configuration.md) ·
[Subscriptions](subscriptions.md)

Albedo replication is a thin wrapper over committed WAL history. The engine
does not own transport, retries, fan-out, or backoff.

The model is:

1. A primary exposes its committed WAL position as a `ReplicationCursor`.
2. The caller requests the committed delta after that cursor.
3. The caller transports the returned bytes.
4. A replica applies the batch by appending its raw WAL frames.
5. A stale generation produces `ReplicationGap` and requires a resnapshot.

This makes the WAL the single physical source of truth for local durability
and replication.

## Design properties

- Batches contain raw WAL frames exactly as stored.
- Only committed frames are visible.
- In-memory buckets do not support replication.
- Replicas are expected to be operationally read-only.
- Applying a batch does not emit oplog entries or recursively replicate it.
- Duplicate delivery is accepted only when the existing range matches
  byte-for-byte.

## Zig API

```zig
pub const ReplicationCursor = extern struct {
    generation: u64,
    next_frame_index: u64,
};

pub fn replicationCursor(self: *Bucket) !ReplicationCursor

pub fn readReplicationBatch(
    self: *Bucket,
    from: ReplicationCursor,
    max_bytes: usize,
    allocator: std.mem.Allocator,
) !?[]u8

pub fn applyReplicationBatch(
    self: *Bucket,
    batch: []const u8,
) !ReplicationCursor
```

## C API

```c
typedef struct albedo_replication_cursor_handle
    albedo_replication_cursor_handle;

albedo_result albedo_replication_cursor(
    albedo_bucket_handle *bucket,
    albedo_replication_cursor_handle **out_cursor);

albedo_result albedo_replication_read(
    albedo_bucket_handle *bucket,
    albedo_replication_cursor_handle *from,
    size_t max_bytes,
    uint8_t **out_batch,
    size_t *out_size);

albedo_result albedo_replication_apply(
    albedo_bucket_handle *bucket,
    const uint8_t *data,
    size_t data_size,
    albedo_replication_cursor_handle **out_cursor);

albedo_result albedo_replication_cursor_close(
    albedo_replication_cursor_handle *cursor);
```

`albedo_replication_read` returns an owned buffer. Release it with
`albedo_free(batch, batch_size)`. C replication cursors are opaque heap
handles; release each one with `albedo_replication_cursor_close`.

## Cursor semantics

`ReplicationCursor` contains:

- `generation` — the WAL-history generation
- `next_frame_index` — the next committed frame expected by the consumer

The frame index uses committed state, not the live append tail. A cursor can
therefore return end-of-stream while unsynced appends exist.

Albedo increments the generation whenever retained WAL history is
intentionally reset. An older generation cannot be satisfied.

| Condition | Zig error / C result | Meaning |
|-----------|----------------------|---------|
| Cursor is at the committed tail | `null` / `ALBEDO_EOS` | No batch is currently available |
| Cursor points past the tail or to a future generation | `InvalidCursor` / `ALBEDO_INVALID_CURSOR` | Cursor is invalid |
| Cursor uses an older generation | `ReplicationGap` / `ALBEDO_REPLICATION_GAP` | Replica must resnapshot |

## Batch format

Version 2 batches contain:

```text
[ReplicationBatchHeader: 112 bytes]
[WAL frame 0]
[WAL frame 1]
...
```

The header fields are:

| Field | Size | Description |
|-------|------|-------------|
| `magic` | 4 bytes | `ARPL` |
| `version` | 2 bytes | Current value: `2` |
| `page_size` | 2 bytes | WAL page size |
| `generation` | 8 bytes | Replication generation |
| `start_frame_index` | 8 bytes | First frame represented by this batch |
| `frame_count` | 8 bytes | Number of frames |
| `wal_salt` | 8 bytes | WAL identity salt |
| `latest_tx_timestamp` | 8 bytes | Latest committed transaction timestamp |
| `bucket_header` | 64 bytes | Committed bucket header state |

The payload is the exact raw frame range from `<bucket>-wal`. Frame integrity
uses the checksum in each WAL frame. Version 1's 48-byte header remains
accepted for compatibility, but new batches are version 2.

## Primary and replica flow

```c
albedo_replication_cursor_handle *cursor = NULL;
albedo_result rc = albedo_replication_cursor(primary, &cursor);
if (rc != ALBEDO_OK) {
    /* handle error */
}

for (;;) {
    uint8_t *batch = NULL;
    size_t batch_size = 0;

    rc = albedo_replication_read(
        primary,
        cursor,
        256 * 1024,
        &batch,
        &batch_size);

    if (rc == ALBEDO_EOS) {
        break;
    }
    if (rc != ALBEDO_HAS_DATA) {
        /* handle invalid cursor, gap, or another error */
        break;
    }

    /* Transport batch and batch_size to the replica. */
    albedo_replication_cursor_handle *next = NULL;
    rc = albedo_replication_apply(replica, batch, batch_size, &next);
    albedo_free(batch, batch_size);

    if (rc == ALBEDO_REPLICATION_GAP) {
        /* Replace the replica with a fresh snapshot. */
        break;
    }
    if (rc != ALBEDO_OK) {
        break;
    }

    albedo_replication_cursor_close(cursor);
    cursor = next;
}

albedo_replication_cursor_close(cursor);
```

The transport may be a network protocol, file transfer, message queue, or any
other byte-preserving mechanism.

## Replica apply rules

- The exact next range applies normally.
- A fully duplicated range is a no-op only when its bytes match.
- A partial overlap, forward gap, or byte mismatch returns `ReplicationGap`.
- Page size, generation, WAL salt, and batch length must match.

After applying a batch, Albedo:

- advances committed WAL state
- inserts frames into the shared WAL index
- invalidates touched cached pages
- restores the replicated bucket header
- reloads index metadata when page 0 changed

## `max_bytes`

- `0` means no caller-imposed limit.
- Any other value returns the largest whole-frame batch that fits.
- The limit must fit the 112-byte header and at least one complete WAL frame.

## Resnapshot after a gap

`ReplicationGap` is the expected recovery path after the primary discards WAL
history needed by the replica.

1. Copy or dump a fresh primary snapshot.
2. Replace and open the replica from that snapshot.
3. Fetch a fresh cursor.
4. Resume incremental batches.

The engine detects the gap but does not move the snapshot.

## Direct WAL tailing

Direct WAL tailing is possible because replication payloads are raw frames,
but callers must reproduce two safety rules:

- expose only committed frames
- track generation resets and resnapshot instead of silently diverging

The replication API exists to package those rules into a small contract.

## Related guides

- [Configuration and durability](configuration.md)
- [Subscriptions](subscriptions.md)
