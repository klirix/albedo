# Subscriptions

[Documentation index](README.md) ·
[Configuration](configuration.md) ·
[Streaming queries](streaming.md) ·
[Query language](query-language.md)

Subscriptions expose a real-time change stream from a WAL-mode bucket. Rather
than scanning pages, a subscription reads a circular oplog ring in the WAL
shared-memory file.

Each entry is an insert, update, or delete envelope. Insert and update
documents are embedded when their payload fits within 1 KiB.

## When to use subscriptions

Prefer a subscription when:

- You need individual operation notifications instead of a document scan.
- You need changes from any writer, including other processes.
- Low polling latency matters more than scan throughput.

Use [streaming queries](streaming.md) when you want the current set of
matching documents rather than a mutation log.

## C API

```c
// Open a WAL-mode bucket.
albedo_bucket_handle *db;
albedo_open("my.bucket", &db);

// Pass an empty BSON query to match all events.
albedo_subscription_handle *sub;
albedo_subscribe(db, query_buf, &sub);

uint8_t *batch_doc;
albedo_result r = albedo_subscribe_poll(sub, &batch_doc, 64);
if (r == ALBEDO_HAS_DATA) {
    // batch_doc is {batch: [{seqno, op, doc_id, ts, doc?}, ...]}.
    // It remains valid until the next poll or close.
} else if (r == ALBEDO_EOS) {
    // No new events; sleep briefly before polling again.
} else if (r == ALBEDO_OPLOG_GAP) {
    // The ring wrapped before this subscriber caught up.
    albedo_subscribe_close(sub);
    albedo_subscribe(db, query_buf, &sub);
}

uint64_t seqno = albedo_subscribe_seqno(sub);

albedo_subscribe_close(sub);
albedo_close(db);
```

## Change event fields

Each element of the `batch` array is a BSON document:

| Field | BSON type | Description |
|-------|-----------|-------------|
| `seqno` | int64 | Monotonically increasing oplog sequence number |
| `op` | string | `"insert"`, `"update"`, or `"delete"` |
| `doc_id` | objectId | Document identifier using the BSON `_id` bytes |
| `ts` | int64 | Unix nanoseconds when the operation was written |
| `doc` | document | Present for insert/update when the inline payload is at most 1 KiB |

## Filtering

Pass the same BSON query format used by `albedo_list`. Insert and update events
whose inline document matches the query are delivered. Delete events without
an inline document always pass through.

See the [query language](query-language.md) for filter syntax.

## Retention and gaps

The oplog ring defaults to 4 MiB and is configured with `oplog_size` when the
bucket is opened. Every handle opening the same WAL must use a compatible
size.

If a writer wraps the ring before a subscriber reads all retained entries,
`albedo_subscribe_poll` returns `ALBEDO_OPLOG_GAP`. Recover by:

1. Closing the stale subscription.
2. Optionally running a full query to rebuild local state.
3. Subscribing again at the current tail.

See [Configuration and durability](configuration.md) for `oplog_size` and WAL
options.
