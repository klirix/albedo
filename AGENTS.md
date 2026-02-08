# Albedo Engine Notes (Agents)

This document summarizes how the core database machinery works based on src/albedo.zig.

## File layout and on-disk format

- The storage unit is a "bucket" file. It starts with a 64-byte BucketHeader, followed by fixed-size pages.
- BucketHeader (64 bytes) fields:
  - magic ("ALBEDO"), version, flags
  - page_size (default 8192)
  - page_count, doc_count, deleted_count
  - reserved padding
- Each page is fixed-size: DEFAULT_PAGE_SIZE (8192) bytes.
  - A 32-byte PageHeader is stored at the start of the page on disk.
  - PageHeader fields: page_type, used_size, page_id, first_readable_byte, reserved.
  - The remaining bytes are page.data (page payload).
- Page types: Data, Index, Free, Meta. Meta page is page 0 and holds index metadata.

## Core in-memory structures

- Page: in-memory struct with PageHeader + data slice + bucket pointer.
- PageCache: LRU cache keyed by page_id.
  - AutoHashMap for lookup and DoublyLinkedList for LRU order.
  - put() evicts oldest entry when capacity is reached.
  - evicted pages are not immediately destroyed because B+ tree nodes may still reference them.
  - clear() frees all cached pages and entries.
- Bucket: central handle for a database file.
  - Maintains file handle, header, page cache, index map, and an RW lock.
  - Tracks replication state and dirty pages for replication batching.

## Bucket lifecycle and initialization

- Bucket.init() delegates to openFile().
- openFileWithOptions():
  - ":memory:" uses an in-memory bucket with no file handle and unlimited page cache.
  - If file does not exist and mode is ReadWrite, createEmptyDBFile() creates it.
  - Otherwise reads BucketHeader, loads meta page (page 0), and loads index metadata.
- createEmptyDBFile():
  - Initializes BucketHeader, creates meta page, and ensures default _id index.
  - Writes meta page and header to disk.
- createInMemoryBucket():
  - Similar to file-backed setup, but stores everything in memory.

## Page IO and caching

- loadPage(page_id):
  - Returns cached page if present.
  - Otherwise reads PageHeader and page data from disk, then caches it.
- writePage(page):
  - Writes PageHeader and data to disk at calculated offset.
  - Tracks dirty pages for replication and triggers periodic fsync.
- createNewPage(page_type):
  - Appends a new page at next page_id and flushes header.
  - Inserts the page into cache.

## Document layout and insertion

- Each document is stored with a 16-byte DocHeader:
  - doc_id (u96), is_deleted flag, reserved (u24).
- Insert flow:
  - If document has no _id, generate ObjectId and set it.
  - Build planned index inserts for all indexes using document values.
  - Find last data page or create a new one.
  - If the current page does not have room for the header, allocate a new page.
  - Write DocHeader followed by BSON bytes, spilling across pages if needed.
  - Insert index entries for all planned values.
  - Increment doc_count and flush header.

## Scanning and reading documents

- ScanIterator walks data pages sequentially.
  - Uses PageIterator (page-type aware) under the hood.
  - step() parses DocHeader at current offset.
  - next() skips deleted docs unless readDeleted is enabled.
  - Handles multi-page documents by allocating a buffer and copying spans.
- readDocAt(page_id, offset):
  - Reads DocHeader to verify not deleted.
  - Reads length from BSON bytes and returns a slice or allocated buffer.
  - Supports multi-page documents.

## Index metadata and persistence

- Meta page (page 0) stores index descriptors:
  - NUL-terminated index path
  - IndexOptions struct
  - root page_id of the index
- loadIndices(meta_page):
  - Reads entries from the meta page and loads each index.
- recordIndexes():
  - Writes all indexes from the map back to the meta page.

## Index building and updates

- ensureIndex(path, options):
  - Returns if index already exists; otherwise builds via buildIndex().
- buildIndex(path, options):
  - Scans all documents using ScanIterator.
  - Extracts indexable values for the path and inserts into a new B+ index.
  - Re-loads the index from root page_id and registers it in the map.
  - Persists index metadata to the meta page.
- Indexable values:
  - Arrays are expanded into multiple values.
  - Strings over MAX_INDEX_STRING_BYTES are rejected.
  - Sparse indexes skip missing values; non-sparse get a null value inserted.

## Query planning and execution

- QueryPlan selects between full scan and index scan.
  - IndexStrategy:
    - range for eq, lt, lte, gt, gte, between, startsWith.
    - points for $in queries (deduplication needed for arrays).
  - Range bounds are tightened when multiple compatible filters appear.
  - Sort coverage: if a sort matches the index path and direction (reverse or not), it is "covered".
- listIterate():
  - Produces a ListIterator that can run eager (prequery + sort) or streaming.
  - Streaming index scans use range or point iterators.
  - Point strategy uses dedup: none (1 value), check_last (2 values), hashmap (many values).
- list():
  - Collects all documents into a list, optionally sorts and applies offset/limit.

## Transform and update behavior

- TransformIterator:
  - Precomputes target locations, then iterates each doc.
  - transform(updated):
    - Tombstones current doc (is_deleted = 1).
    - Deletes index entries for original doc values.
    - Decrements doc_count, increments deleted_count.
    - If updated is non-null, inserts the new document (optionally cloning _id).
  - close():
    - If auto vacuum is enabled and deleted_count > doc_count, runs vacuum().

## Deletion and vacuum

- delete(query):
  - Collects locations via a shared lock scan.
  - For each target:
    - Reads document to compute index values.
    - Tombstones the header and deletes all index entries.
  - Updates doc_count and deleted_count, flushes header.
  - Runs vacuum when deleted_count exceeds doc_count (if enabled).
- vacuum():
  - Creates a temporary bucket file.
  - Rebuilds indexes from meta page definitions.
  - Scans and reinserts all live documents into the new bucket.
  - Replaces the old file with the new one and reloads header/indexes.

## Replication

- writePage() collects dirty page_ids when a replication callback is set.
- notifyDirtyPages():
  - Builds a buffer with header + all dirty pages.
  - Calls the replication callback once with a batch.
  - Retries up to max_replication_retries; otherwise clears dirty pages.
- applyReplicatedBatch(data, page_count):
  - Applies raw pages to the local file and invalidates cache entries.
  - If page 0 is replicated, reloads all indexes.

## Concurrency and memory

- Bucket uses std.Thread.RwLock:
  - Writes (insert, delete, index changes) use exclusive lock.
  - Read/scan paths use shared lock where possible.
- Memory ownership:
  - Many iterators return buffers owned by an arena or allocator passed in.
  - For multi-page docs, readDocAt allocates a buffer that the caller must free.
  - PageCache retains Page objects until cleared.

## Key invariants

- BucketHeader and PageHeader sizes are fixed (64 and 32 bytes).
- Meta page (page 0) is authoritative for index registrations.
- DocHeader reserved must be zero; is_deleted is 0 or 1.
- Documents can span pages; write and read logic must handle continuation.
