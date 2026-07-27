# Streaming queries and cursors

[Documentation index](README.md) ·
[Query language](query-language.md) ·
[Subscriptions](subscriptions.md)

## Streaming queries

`albedo_list` can be consumed as a document stream without exporting a
cursor. Open an iterator, call `albedo_data` or `next()`, and keep the iterator
open if you want to poll it again.

In WAL mode, a live iterator can observe documents written later by another
connection.

```c
albedo_list_handle *it;
albedo_list(db, query_buf, &it);

uint8_t *doc;
while (albedo_data(it, &doc) == ALBEDO_OK) {
    // Consume the current result set.
}

// Later, poll the same iterator for newly visible documents.
while (albedo_data(it, &doc) == ALBEDO_OK) {
    // Consume new documents.
}

albedo_close_iterator(it);
```

Streaming queries work well for:

- Full scans
- Index-range scans such as
  `{ "query": { "age": { "$gte": 30 } } }`
- Long-lived readers observing later writes

Current limitations:

- Queries with `sort` are materialized eagerly and are not stream-shaped.
- `sector` is pagination, not persistent streaming state.
- Point scans such as `$in` work as normal queries but do not support
  resumable cursors.

For low-latency insert, update, and delete envelopes, use
[subscriptions](subscriptions.md) instead of repeatedly scanning pages.

## Streaming cursors

A cursor is an exported snapshot of iterator progress. Export one to close an
iterator and later resume the same stream without replaying documents already
delivered.

### Cursor shape

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

`indexPath` is required for `index_range` mode. The anchor contains both
logical identity and physical location information used to validate the
resume point.

### C API flow

```c
albedo_list_handle *it;
albedo_list(db, query_buf, &it);

uint8_t *doc;
albedo_data(it, &doc);

uint8_t *cursor_buf;
albedo_list_cursor_export(it, &cursor_buf);
albedo_close_iterator(it);

// Build a query containing {"cursor": <cursor_buf>}.
albedo_list(db, resumed_query_buf, &it);
```

The cursor document returned by `albedo_list_cursor_export` is an owned
allocation; release it with `albedo_free`.

### Cursor limitations

Version 1 does not support:

- `sort` with `cursor`
- `sector` with `cursor`
- Point-strategy cursors such as `$in`
- Sharing one cursor iterator across threads
- Snapshot-isolated pagination

Continuation is best effort. A cursor is tied to the current document layout
and stream anchor:

- `vacuum()` invalidates previously exported cursors.
- Resume fails if the anchor can no longer be found.
- Zig reports `InvalidCursor`; the C API reports `ALBEDO_INVALID_CURSOR`.

See the [query language](query-language.md) for supported filter and index-range
syntax.
