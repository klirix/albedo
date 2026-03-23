# WAL-Native Replication

Albedo replication is a thin wrapper over committed WAL history.
There is no replication callback, retry queue, or second page-batch format.

The model is:

1. A primary exposes its committed WAL position as a `ReplicationCursor`.
2. The caller asks for the committed delta since that cursor.
3. The caller transports the bytes however it wants.
4. A replica applies the batch by appending the raw WAL frames into its own WAL.
5. If the cursor is stale after a WAL reset, Albedo returns `ReplicationGap` and the caller resnapshots.

This keeps replication simple and makes the WAL the single physical source of truth.

## Design Notes

- Replication batches contain raw WAL frames exactly as stored on disk.
- Only committed frames are replication-visible. Unsynced WAL appends are not.
- The engine no longer owns transport, retries, fan-out, or backoff.
- Replicas are expected to be operationally read-only.
- In-memory buckets do not support replication because there is no WAL file to tail.

## Public API

### Zig

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

### C

```c
typedef struct albedo_replication_cursor_handle albedo_replication_cursor_handle;

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

`albedo_replication_read` returns an owned heap buffer. Release it with
`albedo_free(batch, batch_size)`.
Replication cursors are opaque handles on the C surface; release them with
`albedo_replication_cursor_close(cursor)`.

## Cursor Semantics

`ReplicationCursor` has two fields:

- `generation`: WAL history generation for this bucket.
- `next_frame_index`: the next committed WAL frame the caller expects to read.

`next_frame_index` is always based on committed WAL state, not live append state.
That means writes appended to the WAL but not yet synced are invisible to replication.

Generation changes when Albedo intentionally invalidates older WAL history, for example after a clean close path that checkpoints and recreates the WAL. When that happens, older cursors are no longer satisfiable.

Error behavior:

- `InvalidCursor`: the cursor points past the current committed frame count or refers to a future generation.
- `ReplicationGap`: the cursor is from an older WAL generation and the caller must resnapshot.
- `EOS`: there are no newer committed frames.

## Batch Format

Each replication batch is:

```text
[ReplicationBatchHeader: 48 bytes]
[WAL frame 0]
[WAL frame 1]
...
```

`ReplicationBatchHeader` fields:

- `magic`: `"ARPL"`
- `version`: `1`
- `page_size`
- `generation`
- `start_frame_index`
- `frame_count`
- `wal_salt`
- `latest_tx_timestamp`

The payload is the raw WAL frame bytes exactly as stored in `<bucket>-wal`.
There is no second replication encoding and no batch checksum; frame integrity continues to rely on the WAL frame headers and checksums.

## Typical Flow

### Primary side

```c
albedo_replication_cursor_handle *cursor = NULL;
albedo_result rc = albedo_replication_cursor(primary, &cursor);
if (rc != ALBEDO_OK) {
    /* handle error */
}

for (;;) {
    uint8_t *batch = NULL;
    size_t batch_size = 0;
    albedo_replication_cursor_handle *replica_cursor = NULL;

    rc = albedo_replication_read(primary, cursor, 256 * 1024, &batch, &batch_size);
    if (rc == ALBEDO_EOS) {
        break;
    }
    if (rc != ALBEDO_HAS_DATA) {
        /* handle InvalidFormat / InvalidCursor / ReplicationGap / Error */
        break;
    }

    /* transport batch to the replica and get its returned cursor */
    rc = albedo_replication_apply(replica, batch, batch_size, &replica_cursor);
    if (rc != ALBEDO_OK) {
        albedo_free(batch, batch_size);
        break;
    }

    albedo_free(batch, batch_size);
    albedo_replication_cursor_close(cursor);
    cursor = replica_cursor;
}

albedo_replication_cursor_close(cursor);
```

### Replica side

```c
albedo_replication_cursor_handle *cursor = NULL;
albedo_result rc = albedo_replication_apply(replica, batch, batch_size, &cursor);

if (rc == ALBEDO_REPLICATION_GAP) {
    /* local WAL history does not line up; resnapshot the replica */
}

albedo_replication_cursor_close(cursor);
```

Replica apply rules:

- Exact next range applies normally.
- Exact duplicate replay is accepted as a no-op if the existing bytes match.
- Any out-of-order overlap or byte mismatch returns `ReplicationGap`.

When a batch is applied, the replica:

- appends the incoming raw frames to its WAL
- updates committed frame count and latest committed timestamp
- inserts the new frames into the shared WAL index
- invalidates cached pages touched by the batch
- reloads the meta page and bucket header when replicated frames include them

It does not emit oplog entries, trigger replication recursively, or rebuild a second physical format.

## `max_bytes` Behavior

- `max_bytes == 0` means "no caller-imposed size limit".
- Otherwise, Albedo returns the largest whole-frame batch that fits.
- If `max_bytes` cannot fit the 48-byte header plus at least one full WAL frame, the call fails with `InvalidFormat` on the C surface (`InvalidArgument` internally).

## Resnapshot on Gap

`ReplicationGap` is the expected recovery path when a replica falls behind older retained WAL history.

Typical recovery:

1. Copy or dump a fresh snapshot from the primary.
2. Open the replica from that snapshot.
3. Fetch a fresh `ReplicationCursor`.
4. Resume streaming incremental WAL batches.

The engine detects the gap, but it does not perform the resnapshot for you.

## Can I Tail the WAL Directly?

Yes. If you want a SQLite or LiteSync-style setup, you can tail the WAL file yourself because replication batches are literally raw WAL frames.

The important caveats are:

- You must only replicate committed frames, not the live append tail.
- You must track WAL generation resets so you can detect stale cursors and resnapshot instead of silently diverging.

That is why the replication API still exists even though the payload is raw WAL. It packages the committed-watermark and generation rules into a small, stable contract.

## Compatibility

This rewrite is intentionally not backward compatible with the old page-batch replication API. The callback-based replication surface has been removed.
