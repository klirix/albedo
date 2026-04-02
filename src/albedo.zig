const std = @import("std");
const builtin = @import("builtin");
pub const bson = @import("bson.zig");
pub const BSONValue = bson.BSONValue;
pub const BSONDocument = bson.BSONDocument;
const mem = std.mem;
const platform = @import("platform.zig");
const ObjectId = @import("object_id.zig").ObjectId;
const ObjectIdGenerator = @import("object_id.zig").ObjectIdGenerator;
const query = @import("query.zig");
pub const Query = query.Query;
const update_program = @import("update_program.zig");
pub const UpdateProgram = update_program.UpdateProgram;
const bindex = @import("bplusindex.zig");
const Index = bindex.Index;
const IndexOptions = bindex.IndexOptions;

pub const wal_mod = @import("wal.zig");
pub const WAL = wal_mod.WAL;

const ALBEDO_MAGIC = "ALBEDO";
const ALBEDO_VERSION: u8 = 1;
const ALBEDO_FLAGS = 0;
pub const DEFAULT_PAGE_SIZE = 8192; // 8kB, or up to 64kB
const DEFAULT_PAGE_CACHE_CAPACITY: usize = 256 * 64; // 64MB cache
const MAX_INDEX_STRING_BYTES: usize = 256;
const DoublyLinkedList = std.DoublyLinkedList;

const BucketHeader = struct {
    magic: [6]u8,
    version: u8,
    flags: u8,
    page_size: u32 = DEFAULT_PAGE_SIZE,
    page_count: u64,
    doc_count: u64,
    deleted_count: u64,
    replication_generation: u64 = 0,
    reserved: [20]u8,

    const byteSize = 64;

    comptime {
        if (@sizeOf(BucketHeader) != byteSize)
            @compileError("BucketHeader must be exactly 64 bytes");
    }

    pub fn init() BucketHeader {
        var header = BucketHeader{
            .magic = undefined,
            .version = ALBEDO_VERSION,
            .flags = ALBEDO_FLAGS,
            .page_size = DEFAULT_PAGE_SIZE,
            .page_count = 0,
            .doc_count = 0,
            .deleted_count = 0,
            .replication_generation = 0,
            .reserved = [_]u8{0} ** 20,
        };

        std.mem.copyForwards(u8, &header.magic, ALBEDO_MAGIC);

        return header;
    }

    pub fn read(memory: []const u8) BucketHeader {
        var header = BucketHeader{
            .magic = undefined,
            .version = memory[6],
            .flags = memory[7],
            .page_size = std.mem.readInt(u32, memory[8..12], .little),
            .page_count = std.mem.readInt(u64, memory[12..20], .little),
            .doc_count = std.mem.readInt(u64, memory[20..28], .little),
            .deleted_count = std.mem.readInt(u64, memory[28..36], .little),
            .replication_generation = std.mem.readInt(u64, memory[36..44], .little),
            .reserved = [_]u8{0} ** 20,
        };

        std.mem.copyForwards(u8, header.magic[0..], ALBEDO_MAGIC);

        return header;
    }

    pub fn toBytes(self: *const BucketHeader) [byteSize]u8 {
        var buffer: [byteSize]u8 = [_]u8{0} ** byteSize;
        std.mem.copyForwards(u8, buffer[0..6], ALBEDO_MAGIC);
        buffer[6] = self.version;
        buffer[7] = self.flags;
        std.mem.writeInt(u32, buffer[8..12], self.page_size, .little);
        std.mem.writeInt(u64, buffer[12..20], self.page_count, .little);
        std.mem.writeInt(u64, buffer[20..28], self.doc_count, .little);
        std.mem.writeInt(u64, buffer[28..36], self.deleted_count, .little);
        std.mem.writeInt(u64, buffer[36..44], self.replication_generation, .little);
        std.mem.copyForwards(u8, buffer[44..64], &self.reserved);
        return buffer;
    }

    pub fn write(self: *const BucketHeader, writer: *std.Io.Writer) !void {
        const bytes = self.toBytes();
        _ = try writer.write(&bytes);
    }
};

pub const ReplicationCursor = extern struct {
    generation: u64,
    next_frame_index: u64,
};

pub const ReplicationBatchHeader = extern struct {
    magic: [4]u8 = .{ 'A', 'R', 'P', 'L' },
    version: u16 = 2,
    page_size: u16 = DEFAULT_PAGE_SIZE,
    generation: u64 = 0,
    start_frame_index: u64 = 0,
    frame_count: u64 = 0,
    wal_salt: u64 = 0,
    latest_tx_timestamp: i64 = 0,
    /// Embedded BucketHeader (v2+).  All-zero for v1 batches.
    bucket_header: [64]u8 = [_]u8{0} ** 64,

    pub const byte_size: usize = 112;
    pub const v1_byte_size: usize = 48;

    comptime {
        if (@sizeOf(ReplicationBatchHeader) != byte_size)
            @compileError("ReplicationBatchHeader must be exactly 112 bytes");
    }

    pub fn toBytes(self: *const ReplicationBatchHeader) [byte_size]u8 {
        var buf: [byte_size]u8 = undefined;
        @memcpy(buf[0..4], &self.magic);
        std.mem.writeInt(u16, buf[4..6], self.version, .little);
        std.mem.writeInt(u16, buf[6..8], self.page_size, .little);
        std.mem.writeInt(u64, buf[8..16], self.generation, .little);
        std.mem.writeInt(u64, buf[16..24], self.start_frame_index, .little);
        std.mem.writeInt(u64, buf[24..32], self.frame_count, .little);
        std.mem.writeInt(u64, buf[32..40], self.wal_salt, .little);
        std.mem.writeInt(i64, buf[40..48], self.latest_tx_timestamp, .little);
        @memcpy(buf[48..112], &self.bucket_header);
        return buf;
    }

    pub fn fromBytes(raw: []const u8) ReplicationBatchHeader {
        var h = ReplicationBatchHeader{
            .magic = raw[0..4].*,
            .version = std.mem.readInt(u16, raw[4..6], .little),
            .page_size = std.mem.readInt(u16, raw[6..8], .little),
            .generation = std.mem.readInt(u64, raw[8..16], .little),
            .start_frame_index = std.mem.readInt(u64, raw[16..24], .little),
            .frame_count = std.mem.readInt(u64, raw[24..32], .little),
            .wal_salt = std.mem.readInt(u64, raw[32..40], .little),
            .latest_tx_timestamp = std.mem.readInt(i64, raw[40..48], .little),
        };
        if (raw.len >= byte_size and h.version >= 2) {
            @memcpy(&h.bucket_header, raw[48..112]);
        }
        return h;
    }
};

const PageType = enum(u8) {
    Data = 1,
    Index = 2,
    Free = 3,
    Meta = 4,
};

const PageHeader = struct {
    page_type: PageType, // 1
    used_size: u16 = 0, // 2
    page_id: u64, // 8
    first_readable_byte: u16, // 2
    reserved: [19]u8, // 19

    pub const byteSize = 32;

    pub fn init(page_type: PageType, id: u64) PageHeader {
        return PageHeader{
            .page_type = page_type,
            .used_size = 0,
            .page_id = id,
            .first_readable_byte = 0,
            .reserved = [_]u8{0} ** 19,
        };
    }

    pub fn write(self: *const PageHeader) [32]u8 {
        var buffer: [32]u8 = [_]u8{0} ** 32;
        buffer[0] = @intFromEnum(self.page_type);
        std.mem.writeInt(u16, buffer[1..3], self.used_size, .little);
        std.mem.writeInt(u64, buffer[3..11], self.page_id, .little);
        std.mem.writeInt(u16, buffer[11..13], self.first_readable_byte, .little);
        std.mem.copyForwards(u8, buffer[13..32], &self.reserved);
        return buffer;
    }

    pub fn read(memory: []const u8) PageHeader {
        const header = PageHeader{
            .page_type = @enumFromInt(std.mem.readInt(u8, memory[0..1], .little)),
            .used_size = std.mem.readInt(u16, memory[1..3], .little),
            .page_id = std.mem.readInt(u64, memory[3..11], .little),
            .first_readable_byte = std.mem.readInt(u16, memory[11..13], .little),
            .reserved = [_]u8{0} ** 19,
        };
        return header;
    }
};

const PageError = error{
    InvalidPageType,
    InvalidPageSize,
    InvalidPageId,
    PageNotFound,
};

pub const Page = struct {
    header: PageHeader,
    data: []u8,
    bucket: *Bucket,

    pub fn init(allocator: std.mem.Allocator, bucket: *Bucket, header: PageHeader) !Page {
        const dataBuffer = try allocator.alloc(u8, DEFAULT_PAGE_SIZE - @sizeOf(PageHeader));
        const newPage = Page{
            .header = header,
            .data = dataBuffer,
            .bucket = bucket,
        };

        @memset(newPage.data, 0);
        return newPage;
    }

    pub fn deinit(self: *Page, allocator: std.mem.Allocator) void {
        allocator.free(self.data);
    }

    // Load indices from the page
};

const PageCache = struct {
    allocator: std.mem.Allocator,
    capacity: usize,
    map: std.AutoHashMap(u64, *Entry),
    lru: DoublyLinkedList = .{},

    const Entry = struct {
        page_id: u64,
        page: *Page,
        node: DoublyLinkedList.Node,
    };

    pub fn init(allocator: std.mem.Allocator, capacity: usize) PageCache {
        const normalized_capacity = if (capacity == 0) 1 else capacity;
        return .{
            .allocator = allocator,
            .capacity = normalized_capacity,
            .map = std.AutoHashMap(u64, *Entry).init(allocator),
            .lru = .{},
        };
    }

    pub fn deinit(self: *PageCache) void {
        self.map.deinit();
    }

    fn promote(self: *PageCache, entry: *Entry) void {
        self.lru.remove(&entry.node);
        self.lru.prepend(&entry.node);
    }

    fn takeOldestEntry(self: *PageCache) ?*Entry {
        const node = self.lru.pop() orelse return null;
        const entry: *Entry = @alignCast(@fieldParentPtr("node", node));
        _ = self.map.remove(entry.page_id);
        return entry;
    }

    pub fn get(self: *PageCache, page_id: u64) ?*Page {
        const entry = self.map.get(page_id) orelse return null;
        self.promote(entry);
        return entry.page;
    }

    pub fn put(self: *PageCache, page_id: u64, page: *Page) !?*Page {
        if (self.capacity == 0) {
            return page;
        }

        if (self.map.get(page_id)) |entry| {
            self.promote(entry);
            const old_page = entry.page;
            entry.page = page;
            return old_page;
        }

        var evicted_page: ?*Page = null;
        if (self.map.count() >= self.capacity) {
            if (self.takeOldestEntry()) |evicted_entry| {
                evicted_page = evicted_entry.page;
                self.allocator.destroy(evicted_entry);
            }
        }

        const entry = try self.allocator.create(Entry);
        entry.* = .{
            .page_id = page_id,
            .page = page,
            .node = .{},
        };
        self.lru.prepend(&entry.node);
        try self.map.put(page_id, entry);

        return evicted_page;
    }

    pub fn remove(self: *PageCache, page_id: u64) ?*Page {
        if (self.map.fetchRemove(page_id)) |kv| {
            const entry = kv.value;
            self.lru.remove(&entry.node);
            const page = entry.page;
            self.allocator.destroy(entry);
            return page;
        }
        return null;
    }

    pub fn clear(self: *PageCache, page_allocator: std.mem.Allocator) void {
        // Clear all entries from LRU list and free pages
        while (self.lru.popFirst()) |node| {
            const entry: *Entry = @alignCast(@fieldParentPtr("node", node));
            _ = self.map.remove(entry.page_id);

            // Deinit and destroy page
            entry.page.deinit(page_allocator);
            page_allocator.destroy(entry.page);

            // Destroy entry
            self.allocator.destroy(entry);
        }

        // Reset state
        self.lru = .{};
        self.map.clearRetainingCapacity();
    }
};

const BucketInitErrors = error{
    InvalidPath,
    InvalidDocId,
    FileNotFound,
    FileOpenError,
    FileReadError,
    FileWriteError,
    LoadIndexError,
    InitializationError,
    OutOfMemory,
    UnexpectedError,
    /// The oplog_size in OpenBucketOptions does not match the size stored in
    /// the existing SHM file.  Open the bucket with the matching oplog_size
    /// or delete the stale `<path>-wal-shm` file and retry.
    OplogSizeMismatch,
};

/// Controls when fsync is called to guarantee write durability.
pub const WriteDurability = union(enum) {
    /// Every write is fsynced immediately (safest, slowest).
    all,
    /// Fsync every N page writes (balanced).
    periodic: u32,
    /// Never fsync automatically; rely on OS page cache.
    /// Data is visible to readers immediately but may be lost on crash.
    /// Use `flush()` for explicit write durability when needed.
    manual,
};

/// Controls how page reads interact with the WAL.
pub const ReadDurability = enum {
    /// Always consult the WAL before returning a cached page.
    /// Required for multi-process readers that must see writes from
    /// other connections.  This is the safe default.
    shared,
    /// Trust the local page cache: return cached pages without
    /// consulting the WAL.  The WAL is only checked on cache misses.
    /// Use this for single-process workloads where all writes and
    /// reads go through the same `Bucket` handle — it avoids the
    /// SHM lock + skip-list traversal + 8 KB pread on every
    /// index-node read and restores pre-WAL query performance.
    process,
};

pub const Bucket = struct {
    file: ?platform.FileHandle = null,
    path: []const u8,
    allocator: std.mem.Allocator,
    header: BucketHeader,
    pageCache: PageCache,
    rwlock: std.Thread.RwLock = .{},
    indexes: std.StringHashMap(*Index),
    autoVaccuum: bool = true,
    objectIdGenerator: ObjectIdGenerator,
    in_memory: bool = false,
    writes_since_sync: u32 = 0,
    write_durability: WriteDurability = .{ .periodic = 100 },
    wal_pending_pages: std.AutoHashMap(u64, void),
    wal_header_pending: bool = false,
    cached_last_data_page: ?*Page = null,
    read_durability: ReadDurability = .shared,
    oplog_size: u32 = WAL.DEFAULT_OPLOG_REGION_SIZE,
    wal: ?WAL = null,
    active_tx: ?*Transaction = null,
    /// Local snapshot of the WAL SHM checkpoint_generation.  When the
    /// SHM value is higher (another connection checkpointed), the page
    /// cache is stale and must be cleared before reading.
    wal_checkpoint_generation: u64 = 0,
    /// Number of frames after which the WAL is automatically
    /// checkpointed.  0 disables auto-checkpoint.
    wal_auto_checkpoint: u64 = 1000,
    /// When true, mutation methods skip oplog emission (used by
    /// TransformIterator to avoid double-emitting for updates).
    _suppress_oplog: bool = false,

    const PageIterator = struct {
        bucket: *Bucket,
        index: u64 = 0,
        type: PageType,
        reverse: bool = false,

        pub fn next(self: *PageIterator) !?*Page {
            var found = false;
            var page: *Page = undefined;
            while (!found) {
                if (self.reverse) {
                    if (self.index == 0) {
                        return null;
                    }
                    self.index -= 1;
                    page = try self.bucket.loadPage(self.index);
                    found = page.header.page_type == self.type;
                } else {
                    if (self.index >= self.bucket.header.page_count) {
                        // WAL mode: another connection may have created new
                        // pages since we last checked.  Refresh the header
                        // from the mmap'd WAL index before giving up.
                        self.bucket.loadHeaderFromWal();
                        if (self.index >= self.bucket.header.page_count) {
                            return null;
                        }
                    }
                    page = try self.bucket.loadPage(self.index);
                    found = page.header.page_type == self.type;
                    self.index += 1;
                }
            }
            return page;
        }
    };

    // Update init to include allocator
    pub fn init(allocator: std.mem.Allocator, path: []const u8) !Bucket {
        return Bucket.openFile(allocator, path);
    }

    const BucketFileMode = enum {
        ReadOnly,
        ReadWrite,
    };

    pub const OpenBucketOptions = struct {
        buildIdIndex: bool = false,
        mode: BucketFileMode = BucketFileMode.ReadWrite,
        auto_vaccuum: bool = true,
        page_cache_capacity: usize = DEFAULT_PAGE_CACHE_CAPACITY,
        /// Enable the Write-Ahead Log for crash recovery and MVCC.
        /// When false, pages are written directly to the main DB file
        /// (simpler but no cross-process MVCC or crash safety).
        wal: bool = true,
        /// Size of the oplog circular ring buffer in bytes.
        /// 0 disables the oplog entirely.
        /// Must match the value used when the SHM file was first created;
        /// a mismatch will cause `openFileWithOptions` to return an error.
        /// Older SHM files (where this field was in reserved storage and
        /// therefore zero) will be seamlessly re-initialized with this value.
        oplog_size: u32 = WAL.DEFAULT_OPLOG_REGION_SIZE,
        /// Controls when fsync is called.
        ///  - .all        — fsync every write (safest, slowest)
        ///  - .periodic(N) — fsync every N page writes
        ///  - .manual      — never auto-fsync; call flush() manually
        write_durability: WriteDurability = .{ .periodic = 100 },
        /// Controls how page reads interact with the WAL.
        ///  - .shared  — always consult WAL (safe for multi-process readers)
        ///  - .process — trust local cache, WAL only on miss (fast single-process)
        read_durability: ReadDurability = .shared,
        /// Number of WAL frames after which an automatic checkpoint is triggered
        /// at the end of a commit.  0 disables auto-checkpointing entirely.
        wal_auto_checkpoint: u64 = 1000,
    };

    const IndexRootSnapshot = struct {
        index: *Index,
        root_page_id: u64,
    };

    const DeferredOplogEntry = struct {
        op: WAL.OpKind,
        doc_id: u96,
        payload: ?[]u8 = null,

        fn deinit(self: *DeferredOplogEntry, allocator: std.mem.Allocator) void {
            if (self.payload) |payload| {
                allocator.free(payload);
            }
            self.payload = null;
        }
    };

    pub const Transaction = struct {
        bucket: *Bucket,
        pending_pages: std.AutoHashMap(u64, *Page),
        header_snapshot: BucketHeader,
        header_dirty: bool = false,
        cached_last_data_page_snapshot: ?*Page,
        index_root_snapshots: []IndexRootSnapshot,
        deferred_oplog: std.ArrayList(DeferredOplogEntry) = .{},
        open_transforms: usize = 0,
        active: bool = true,

        pub fn insert(self: *Transaction, insertable: bson.BSONDocument) !DocInsertResult {
            try self.ensureActive();
            return self.bucket.insertLocked(insertable, false);
        }

        pub fn delete(self: *Transaction, q: query.Query) !void {
            try self.ensureActive();
            try self.bucket.deleteLocked(q, false);
        }

        pub fn transformIterate(self: *Transaction, arena: *std.heap.ArenaAllocator, q: query.Query) !*TransformIterator {
            try self.ensureActive();
            return self.bucket.transformIterateInternal(arena, q, self);
        }

        pub fn transfigurate(self: *Transaction, q: query.Query, program: UpdateProgram) !usize {
            try self.ensureActive();

            var arena = std.heap.ArenaAllocator.init(self.bucket.allocator);
            defer arena.deinit();

            var iter = try self.transformIterate(&arena, q);
            var closed = false;
            defer {
                if (!closed) {
                    iter.close() catch {};
                }
            }

            const updated = try iter.transfigurateAll(program);
            try iter.close();
            closed = true;
            return updated;
        }

        pub fn commit(self: *Transaction) !void {
            try self.ensureActive();
            if (self.open_transforms != 0) return error.TransactionBusy;

            var it = self.pending_pages.iterator();
            while (it.next()) |entry| {
                const page_id = entry.key_ptr.*;
                const page = entry.value_ptr.*;
                if (try self.bucket.pageCache.put(page_id, page)) |evicted| {
                    if (evicted.header.page_id == page_id) {
                        evicted.deinit(self.bucket.allocator);
                        self.bucket.allocator.destroy(evicted);
                    }
                }
                try self.bucket.wal_pending_pages.put(page_id, {});
            }

            if (self.header_dirty) {
                self.bucket.wal_header_pending = true;
            }

            self.bucket.commitWalTransaction();

            for (self.deferred_oplog.items) |entry| {
                self.bucket.emitOplogEntry(entry.op, entry.doc_id, entry.payload);
            }

            self.finishActiveState();
        }

        pub fn rollback(self: *Transaction) !void {
            try self.ensureActive();
            if (self.open_transforms != 0) return error.TransactionBusy;
            self.abort(false);
        }

        pub fn close(self: *Transaction) !void {
            if (self.active) {
                try self.rollback();
            }
            self.deinit();
        }

        pub fn deinit(self: *Transaction) void {
            if (self.active) {
                self.abort(true);
            }
            self.pending_pages.deinit();
            self.clearDeferredOplog();
            self.bucket.allocator.free(self.index_root_snapshots);
            self.bucket.allocator.destroy(self);
        }

        fn stageOplog(self: *Transaction, op: WAL.OpKind, doc_id: u96, payload: ?[]const u8) !void {
            const owned_payload = if (payload) |bytes|
                try self.bucket.allocator.dupe(u8, bytes)
            else
                null;

            try self.deferred_oplog.append(self.bucket.allocator, .{
                .op = op,
                .doc_id = doc_id,
                .payload = owned_payload,
            });
        }

        fn loadPageForWrite(self: *Transaction, page_id: u64) !*Page {
            if (self.pending_pages.get(page_id)) |page| {
                return page;
            }

            const source_page = try self.bucket.loadPage(page_id);
            const page = try self.bucket.allocator.create(Page);
            errdefer self.bucket.allocator.destroy(page);
            page.* = try Page.init(self.bucket.allocator, self.bucket, source_page.header);
            @memcpy(page.data, source_page.data);
            try self.pending_pages.put(page_id, page);

            if (self.bucket.cached_last_data_page) |cached| {
                if (cached.header.page_id == page_id) {
                    self.bucket.cached_last_data_page = page;
                }
            }

            return page;
        }

        fn ensureActive(self: *Transaction) !void {
            if (!self.active or self.bucket.active_tx != self) {
                return error.InvalidTransaction;
            }
        }

        fn finishActiveState(self: *Transaction) void {
            if (!self.active) return;
            self.active = false;
            self.bucket.active_tx = null;
            self.bucket.rwlock.unlock();
        }

        fn abort(self: *Transaction, force: bool) void {
            if (!self.active) return;
            if (!force and self.open_transforms != 0) return;

            self.bucket.header = self.header_snapshot;
            self.bucket.cached_last_data_page = self.cached_last_data_page_snapshot;
            for (self.index_root_snapshots) |snapshot| {
                snapshot.index.root_page_id = snapshot.root_page_id;
            }

            var it = self.pending_pages.iterator();
            while (it.next()) |entry| {
                const page = entry.value_ptr.*;
                page.deinit(self.bucket.allocator);
                self.bucket.allocator.destroy(page);
            }
            self.pending_pages.clearRetainingCapacity();
            self.clearDeferredOplog();
            self.finishActiveState();
        }

        fn clearDeferredOplog(self: *Transaction) void {
            for (self.deferred_oplog.items) |*entry| {
                entry.deinit(self.bucket.allocator);
            }
            self.deferred_oplog.deinit(self.bucket.allocator);
            self.deferred_oplog = .{};
        }
    };

    fn loadIndices(self: *Bucket, page: *const Page) !void {
        var reader = std.io.Reader.fixed(page.data);
        while (true) {
            // Stop when we hit the first NUL at the beginning (no more entries)
            const b = try reader.peekByte();
            if (b == 0) break;

            // Read the index path (NUL-terminated); returned slice is inclusive of NUL
            const path_inclusive = reader.takeDelimiterInclusive(0) catch return PageError.InvalidPageSize;
            if (path_inclusive.len == 0) break; // defensive
            // Strip trailing NUL so map key does not include it
            const path_no_nul = path_inclusive[0 .. path_inclusive.len - 1];
            const key = try self.allocator.dupe(u8, path_no_nul);
            errdefer self.allocator.free(key);

            // Read index options
            const options = reader.takeStruct(IndexOptions, .little) catch return PageError.InvalidPageSize;

            // Read the index page ID
            const page_id = reader.takeInt(u64, .little) catch return PageError.InvalidPageSize;

            const idx = Index.loadWithOptions(self, page_id, options) catch {
                // std.debug.print("Failed to load index at page {d} for path {s}\n", .{ page_id, key });
                return BucketInitErrors.LoadIndexError;
            };

            // Insert the index into the hash map
            try self.indexes.put(key, idx);
        }
    }

    fn recordIndexes(self: *Bucket) !void {
        const meta_page = try self.loadPage(0);

        var buffer: [DEFAULT_PAGE_SIZE - @sizeOf(PageHeader)]u8 = undefined;
        var writer = std.io.Writer.fixed(&buffer);

        var it = self.indexes.iterator();

        while (it.next()) |entry| {
            const path_full = entry.key_ptr.*;
            // Sanitize: ensure we don't have a trailing NUL stored as part of the key
            const path = if (path_full.len > 0 and path_full[path_full.len - 1] == 0)
                path_full[0 .. path_full.len - 1]
            else
                path_full;
            const index = entry.value_ptr.*;

            try writer.writeAll(path);
            try writer.writeByte(0);
            try writer.writeStruct(index.options, .little);
            try writer.writeInt(u64, index.root_page_id, .little);
        }
        const unused = writer.unusedCapacityLen();
        try writer.splatByteAll(0, unused); // Null-terminate the list
        @memcpy(meta_page.data, &buffer);

        try self.writePage(meta_page);
    }

    pub fn beginTransaction(self: *Bucket) !*Transaction {
        if (self.active_tx != null) return error.TransactionActive;
        self.rwlock.lock();
        errdefer self.rwlock.unlock();
        if (self.active_tx != null) return error.TransactionActive;

        var root_snapshots = std.ArrayList(IndexRootSnapshot){};
        defer root_snapshots.deinit(self.allocator);

        var idx_iter = self.indexes.iterator();
        while (idx_iter.next()) |entry| {
            try root_snapshots.append(self.allocator, .{
                .index = entry.value_ptr.*,
                .root_page_id = entry.value_ptr.*.root_page_id,
            });
        }

        const tx = try self.allocator.create(Transaction);
        errdefer self.allocator.destroy(tx);

        tx.* = .{
            .bucket = self,
            .pending_pages = std.AutoHashMap(u64, *Page).init(self.allocator),
            .header_snapshot = self.header,
            .cached_last_data_page_snapshot = self.cached_last_data_page,
            .index_root_snapshots = try self.allocator.dupe(IndexRootSnapshot, root_snapshots.items),
        };

        self.active_tx = tx;
        return tx;
    }

    inline fn ensureNoActiveTransaction(self: *Bucket) !void {
        if (self.active_tx != null) {
            return error.TransactionActive;
        }
    }

    inline fn currentTransaction(self: *Bucket) ?*Transaction {
        return self.active_tx;
    }

    pub fn loadPageForWrite(self: *Bucket, page_id: u64) !*Page {
        if (self.active_tx) |tx| {
            return tx.loadPageForWrite(page_id);
        }
        return self.loadPage(page_id);
    }

    fn stageOplogEntry(self: *Bucket, op: WAL.OpKind, doc_id: u96, payload: ?[]const u8) !void {
        if (self.active_tx) |tx| {
            try tx.stageOplog(op, doc_id, payload);
            return;
        }
        self.emitOplogEntry(op, doc_id, payload);
    }

    pub fn ensureIndex(self: *Bucket, path: []const u8, options: IndexOptions) !void {
        try self.ensureNoActiveTransaction();
        if (self.indexes.contains(path)) {
            // Index already exists
            return;
        }
        // const newIndex = try Index.create(self);
        // newIndex.options = options;
        // const key = try self.allocator.dupe(u8, path);
        // errdefer self.allocator.free(key);
        // try self.indexes.put(key, newIndex);
        // try self.recordIndexes();
        try self.buildIndex(path, options);
    }
    const IndexEntry = struct {
        key: []const u8,
        value: *Index,
    };

    const IndexInfo = struct {
        bucket: *Bucket,
        indexes: []IndexEntry,
        pub fn deinit(self: *IndexInfo) void {
            self.bucket.allocator.free(self.indexes);
        }
    };

    pub fn listIndexes(self: *Bucket) !IndexInfo {
        const count = self.indexes.count();

        var entries = try self.allocator.alloc(IndexEntry, count);
        var recordIterator = self.indexes.iterator();
        for (0..count) |i| {
            const entry = recordIterator.next() orelse unreachable;
            entries[i] = IndexEntry{
                .key = entry.key_ptr.*,
                .value = entry.value_ptr.*,
            };
        }
        return IndexInfo{
            .bucket = self,
            .indexes = entries,
        };
    }

    pub fn dropIndex(self: *Bucket, path: []const u8) !void {
        try self.ensureNoActiveTransaction();
        self.rwlock.lock();
        defer self.rwlock.unlock();

        var removed = self.indexes.fetchRemove(path) orelse return error.IndexNotFound;
        errdefer {
            // Attempt to restore the removed entry if anything fails after removal.
            self.indexes.put(removed.key, removed.value) catch {
                removed.value.deinit();
                self.allocator.free(@constCast(removed.key));
            };
        }

        try self.recordIndexes();

        removed.value.deinit();
        self.allocator.free(@constCast(removed.key));
    }

    pub fn openFile(ally: std.mem.Allocator, path: []const u8) BucketInitErrors!Bucket {
        return Bucket.openFileWithOptions(ally, path, .{});
    }

    fn createEmptyDBFile(path: []const u8, ally: mem.Allocator, options: OpenBucketOptions) BucketInitErrors!Bucket {
        var new_file = platform.openFile(ally, path, .{
            .read = true,
            .write = true,
            .create = true,
            .truncate = true,
        }) catch |err| switch (err) {
            error.FileNotFound => unreachable,
            else => {
                // std.debug.print("Failed to create file: {s}, error: {any}\n", .{ path, err });
                return BucketInitErrors.FileOpenError;
            },
        };

        const stored_path = ally.dupe(u8, path) catch {
            new_file.close();
            return BucketInitErrors.OutOfMemory;
        };

        const generator = ObjectIdGenerator.init() catch {
            new_file.close();
            ally.free(stored_path);
            return BucketInitErrors.UnexpectedError;
        };

        // Bootstrap the file WITHOUT the WAL so that the header and meta
        // page are written directly to the main file.  This ensures the
        // DB file always has a valid baseline, just like SQLite.
        var bucket = Bucket{
            .file = new_file,
            .path = stored_path,
            .header = .init(),
            .allocator = ally,
            .pageCache = PageCache.init(ally, options.page_cache_capacity),
            .indexes = .init(ally),
            .autoVaccuum = options.auto_vaccuum,
            .objectIdGenerator = generator,
            .wal_pending_pages = std.AutoHashMap(u64, void).init(ally),
            .write_durability = options.write_durability,
            .read_durability = options.read_durability,
            .oplog_size = options.oplog_size,
            .wal_auto_checkpoint = options.wal_auto_checkpoint,
            .wal = null,
        };
        bucket.flushHeader() catch return BucketInitErrors.FileWriteError;

        const meta = bucket.createNewPage(.Meta) catch return BucketInitErrors.InitializationError;
        bucket.ensureIndex("_id", .{ .unique = 1 }) catch return BucketInitErrors.InitializationError;

        bucket.writePage(meta) catch return BucketInitErrors.FileWriteError;

        bucket.commitWalTransaction();

        // Sync the baseline to disk before activating the WAL.
        if (bucket.file) |*f| f.sync() catch {};

        // Now attach the WAL for subsequent operations (if enabled).
        bucket.wal = if (options.wal) try initWal(ally, path, options.oplog_size, bucket.header.replication_generation) else null;

        bucket.rwlock = .{};

        return bucket;
    }

    fn createInMemoryBucket(ally: mem.Allocator, options: OpenBucketOptions) BucketInitErrors!Bucket {
        const generator = ObjectIdGenerator.init() catch return BucketInitErrors.UnexpectedError;
        var bucket = Bucket{
            .file = null,
            .path = ally.dupe(u8, ":memory:") catch return BucketInitErrors.OutOfMemory,
            .header = .init(),
            .allocator = ally,
            .pageCache = PageCache.init(ally, std.math.maxInt(usize)),
            .indexes = .init(ally),
            .autoVaccuum = options.auto_vaccuum,
            .objectIdGenerator = generator,
            .in_memory = true,
            .wal_pending_pages = std.AutoHashMap(u64, void).init(ally),
            .read_durability = options.read_durability,
            .oplog_size = options.oplog_size,
            .wal_auto_checkpoint = options.wal_auto_checkpoint,
        };

        const meta = bucket.createNewPage(.Meta) catch return BucketInitErrors.InitializationError;
        bucket.ensureIndex("_id", .{ .unique = 1 }) catch return BucketInitErrors.InitializationError;
        bucket.writePage(meta) catch return BucketInitErrors.FileWriteError;

        bucket.rwlock = .{};

        return bucket;
    }

    // Path may be relative or absolute
    pub fn openFileWithOptions(ally: std.mem.Allocator, path: []const u8, options: OpenBucketOptions) BucketInitErrors!Bucket {
        if (mem.eql(u8, path, ":memory:")) {
            return createInMemoryBucket(ally, options);
        }

        var file = platform.openFile(ally, path, .{
            .read = true,
            .write = options.mode == .ReadWrite,
        }) catch |err| switch (err) {
            error.FileNotFound => {
                if (options.mode != .ReadWrite) return BucketInitErrors.FileNotFound;
                return createEmptyDBFile(path, ally, options);
            },
            else => return BucketInitErrors.FileOpenError,
        };

        var header_bytes: [BucketHeader.byteSize]u8 = undefined;
        file.preadAll(header_bytes[0..], 0) catch |err| {
            file.close();
            return switch (err) {
                error.FileNotFound => BucketInitErrors.FileReadError,
                else => BucketInitErrors.FileReadError,
            };
        };

        const stored_path = ally.dupe(u8, path) catch {
            file.close();
            return BucketInitErrors.OutOfMemory;
        };

        const generator = ObjectIdGenerator.init() catch {
            file.close();
            ally.free(stored_path);
            return BucketInitErrors.UnexpectedError;
        };

        const existing_header = BucketHeader.read(header_bytes[0..BucketHeader.byteSize]);
        const wal_instance: ?WAL = if (options.wal) try initWal(ally, path, options.oplog_size, existing_header.replication_generation) else null;

        var bucket = Bucket{
            .file = file,
            .path = stored_path,
            .allocator = ally,
            .header = existing_header,
            .pageCache = PageCache.init(ally, options.page_cache_capacity),
            .indexes = .init(ally),
            .autoVaccuum = options.auto_vaccuum,
            .objectIdGenerator = generator,
            .wal_pending_pages = std.AutoHashMap(u64, void).init(ally),
            .write_durability = options.write_durability,
            .read_durability = options.read_durability,
            .oplog_size = options.oplog_size,
            .wal_auto_checkpoint = options.wal_auto_checkpoint,
            .wal = wal_instance,
        };
        errdefer {
            if (bucket.file) |*fh| fh.close();
            bucket.pageCache.deinit();
            bucket.indexes.deinit();
            ally.free(bucket.path);
            if (bucket.wal) |*w| w.deinit();
        }

        // In WAL mode the SHM index (rebuilt inside WAL.init) already
        // makes un-checkpointed frames visible to loadPage.  We only
        // need to pick up the latest BucketHeader from the WAL (if any)
        // so that page_count / doc_count are correct.
        bucket.loadHeaderFromWal();

        const meta = bucket.loadPage(0) catch {
            return BucketInitErrors.FileReadError;
        };

        bucket.loadIndices(meta) catch {
            return BucketInitErrors.LoadIndexError;
        };

        return bucket;
    }

    /// Load the most recent BucketHeader from the WAL (if one exists).
    /// Called on open so that page_count / doc_count reflect any writes
    /// that were committed to the WAL but not yet checkpointed.
    fn loadHeaderFromWal(self: *Bucket) void {
        // Never call during an in-flight transaction: the pending header has a
        // higher page_count than whatever is in the WAL, so reading it would
        // silently decrease header.page_count and cause createNewPage to
        // re-use an already-allocated page_id.
        if (self.wal_header_pending) return;
        if (self.wal_pending_pages.count() > 0) return;
        const w = &(self.wal orelse return);

        // Snapshot the current SHM checkpoint generation.
        self.wal_checkpoint_generation = @atomicLoad(u64, &w.index.shmHeader().checkpoint_generation, .acquire);

        // Fast path: the writer publishes page_count/doc_count to SHM
        // atomically, so readers can pick them up without any disk I/O.
        const shm = w.index.shmHeader();
        const shm_pc = @atomicLoad(u64, &shm.shm_page_count, .acquire);
        const shm_dc = @atomicLoad(u64, &shm.shm_doc_count, .acquire);
        if (shm_pc > 0) {
            if (shm_pc > self.header.page_count) {
                self.header.page_count = shm_pc;
            }
            if (shm_dc > self.header.doc_count) {
                self.header.doc_count = shm_dc;
            }
            return;
        }

        // Fallback: re-read the embedded BucketHeader from the WAL file
        // on disk (used when the SHM was freshly created, e.g. after a
        // crash where the writer had already synced the WAL header).
        if (w.readBucketHeader() catch null) |bh| {
            const wal_header = BucketHeader.read(&bh);
            if (wal_header.page_count > self.header.page_count) {
                self.header.page_count = wal_header.page_count;
            }
            if (wal_header.doc_count > self.header.doc_count) {
                self.header.doc_count = wal_header.doc_count;
            }
            return;
        }

        // Fallback for v2 WALs: try the HEADER_PAGE_ID sentinel frame.
        const data = w.page_data(WAL.HEADER_PAGE_ID, std.math.maxInt(i64)) catch return;
        const wal_header = BucketHeader.read(data[0..BucketHeader.byteSize]);
        if (wal_header.page_count > self.header.page_count) {
            self.header.page_count = wal_header.page_count;
        }
        if (wal_header.doc_count > self.header.doc_count) {
            self.header.doc_count = wal_header.doc_count;
        }
    }

    /// Apply every WAL frame to the main database file and write the
    /// current BucketHeader.  Safe to call even when other readers are
    /// active — they will detect the new checkpoint_generation in SHM
    /// and invalidate their page caches.
    /// Flush all WAL frames to the main database file and truncate the WAL.
    /// This is a no-op when the WAL is not active or has no pending frames.
    pub fn checkpoint(self: *Bucket) void {
        if (self.wal) |*w| {
            w.sync() catch {};
        }
        self.checkpointWal();
        if (self.wal) |*w| {
            w.checkpoint() catch {};
        }
        if (self.wal) |*w| {
            self.wal_checkpoint_generation = @atomicLoad(u64, &w.index.shmHeader().checkpoint_generation, .acquire);
        }
    }

    fn checkpointWal(self: *Bucket) void {
        const w = &(self.wal orelse return);
        if (w.header.frame_count == 0 and w.live_frame_count == 0) return;

        const f = if (self.file) |*fh| fh else return;

        var it = w.frames();

        while (it.next() catch null) |frame| {
            // Skip legacy v2 sentinel frames — their data is already
            // captured in the in-memory BucketHeader via the embedded
            // WAL header field.
            if (frame.header.page_id == WAL.HEADER_PAGE_ID) continue;

            const offset = BucketHeader.byteSize + frame.header.page_id * DEFAULT_PAGE_SIZE;
            f.pwriteAll(frame.data, offset) catch return;
        }

        // Write the current in-memory header (most up-to-date).
        const bytes = self.header.toBytes();
        f.pwriteAll(bytes[0..], 0) catch return;
        f.sync() catch return;
    }

    /// Build the WAL path (<db_path>-wal) and open/create the WAL.
    /// Returns `error.OplogSizeMismatch` when the SHM file was created with a
    /// different non-zero oplog_size, so the caller can surface it properly.
    fn initWal(ally: std.mem.Allocator, db_path: []const u8, oplog_size: u32, generation: u64) BucketInitErrors!WAL {
        const wal_path = ally.alloc(u8, db_path.len + 4) catch return BucketInitErrors.OutOfMemory;
        defer ally.free(wal_path);
        @memcpy(wal_path[0..db_path.len], db_path);
        @memcpy(wal_path[db_path.len..], "-wal");
        return WAL.init(ally, wal_path, oplog_size, generation) catch |err| switch (err) {
            WAL.Error.WalOplogSizeMismatch => return BucketInitErrors.OplogSizeMismatch,
            WAL.Error.WalOpenFailed,
            WAL.Error.WalReadFailed,
            WAL.Error.WalCorrupted,
            WAL.Error.WalRecoveryFailed,
            WAL.Error.WalWriteFailed,
            WAL.Error.WalSyncFailed,
            WAL.Error.WalIndexFailed,
            WAL.Error.WalPageNotFound,
            => return BucketInitErrors.InitializationError,
        };
    }

    fn persistHeaderToMainFile(self: *Bucket) void {
        if (self.in_memory) return;
        const file = if (self.file) |*f| f else return;
        const bytes = self.header.toBytes();
        file.pwriteAll(bytes[0..], 0) catch return;
        file.sync() catch return;
    }

    fn bumpReplicationGeneration(self: *Bucket) void {
        const next_generation = self.nextReplicationGeneration() catch return;
        self.header.replication_generation = next_generation;
        self.persistHeaderToMainFile();
        if (self.wal) |*w| {
            w.setGeneration(next_generation) catch {};
        }
    }

    fn nextReplicationGeneration(self: *const Bucket) error{ReplicationGenerationExhausted}!u64 {
        return std.math.add(u64, self.header.replication_generation, 1) catch error.ReplicationGenerationExhausted;
    }

    fn flushHeader(self: *Bucket) !void {
        if (self.in_memory) {
            return;
        }

        if (self.active_tx) |tx| {
            tx.header_dirty = true;
            return;
        }

        // Always defer: commitWalTransaction() will write the final header
        // to either the WAL or the main DB file once per logical transaction.
        // This prevents multiple costly small writes to file offset 0 during
        // B+ tree splits and index builds.
        self.wal_header_pending = true;
    }

    pub fn buildIndex(self: *Bucket, path: []const u8, options: IndexOptions) !void {
        try self.ensureNoActiveTransaction();
        self.rwlock.lock();
        defer {
            self.rwlock.unlock();
        }

        if (self.indexes.contains(path)) {
            return;
        }

        // Use arena allocator for all temporary allocations during index building
        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const temp_allocator = arena.allocator();

        var index = try Index.create(self);
        var index_owner = true;
        defer if (index_owner) index.deinit();
        index.options = options;

        var scanner = try ScanIterator.init(self, temp_allocator);
        defer scanner.deinit();

        while (true) {
            const maybe_doc = try scanner.next();
            if (maybe_doc == null) break;
            const doc_result = maybe_doc.?;

            var doc = BSONDocument{ .buffer = doc_result.data };

            var values = std.ArrayList(BSONValue){};
            defer values.deinit(temp_allocator);

            const has_values = try self.gatherIndexValuesForPath(&doc, path, options, &values);
            if (!has_values) continue;

            // No need to clone values since they're in the arena and will be used immediately
            const location = Index.DocumentLocation{
                .pageId = doc_result.page_id,
                .offset = doc_result.offset,
            };

            for (values.items) |value| {
                try index.insert(value, location);
            }
        }

        const root_id = index.root_page_id;
        index.deinit();
        index_owner = false;

        var final_index = try Index.loadWithOptions(self, root_id, options);
        var final_index_owner = true;
        defer if (final_index_owner) final_index.deinit();

        const key = try self.allocator.dupe(u8, path);
        var key_owner = true;
        defer if (key_owner) self.allocator.free(key);

        var inserted_into_map = false;
        errdefer if (inserted_into_map) {
            _ = self.indexes.remove(path);
        };

        try self.indexes.put(key, final_index);
        inserted_into_map = true;

        self.bindIndex(final_index);

        try self.recordIndexes();

        // Commit all the B+ tree page writes accumulated during index build to WAL.
        self.commitWalTransaction();

        final_index_owner = false;
        key_owner = false;
        inserted_into_map = false;
    }

    // Previous openFile implementation remains the same...

    pub fn loadPage(self: *Bucket, page_id: u64) !*Page {
        if (self.active_tx) |tx| {
            if (tx.pending_pages.get(page_id)) |page| {
                return page;
            }
        }

        if (self.in_memory) {
            if (self.pageCache.get(page_id)) |page| {
                return page;
            }
            return PageError.PageNotFound;
        }

        // Check if another connection checkpointed the WAL.  If so,
        // the main DB file was updated and our cached pages are stale.
        self.maybeInvalidateCacheOnCheckpoint();

        // read_durability == .process  →  trust the local cache; only
        //   consult the WAL on a cache miss.  This avoids SHM lock +
        //   skip-list lookup + 8 KB pread + checksum on every B+ tree
        //   node read and restores pre-WAL index performance.
        //
        // read_durability == .shared  →  always ask the WAL first so
        //   that pages written by another process are visible even if
        //   the local cache holds a stale copy.
        switch (self.read_durability) {
            .process => {
                // Fast path: cache hit → done.
                // Pending pages are always in cache (they were modified in-memory
                // and commitWalTransaction hasn't run yet), so cache is authoritative.
                if (self.pageCache.get(page_id)) |page| {
                    return page;
                }
                // Cache miss — try the WAL, then the main DB file.
                if (self.wal) |*w| {
                    if (w.page_data(page_id, std.math.maxInt(i64))) |data| {
                        return try self.cachePageFromWal(page_id, data);
                    } else |err| switch (err) {
                        error.WalPageNotFound => {},
                        else => {},
                    }
                }
            },
            .shared => {
                // If this page has pending (uncommitted-to-WAL) writes, the
                // in-memory page cache holds the authoritative state.  Fetching
                // from the WAL here would return an older committed version and
                // overwrite the pending in-memory modifications.
                if (self.wal_pending_pages.contains(page_id)) {
                    if (self.pageCache.get(page_id)) |page| {
                        return page;
                    }
                }

                // Always consult the WAL first for cross-process visibility.
                if (self.wal) |*w| {
                    if (w.page_data(page_id, std.math.maxInt(i64))) |data| {
                        const header = PageHeader.read(data[0..PageHeader.byteSize]);

                        if (self.pageCache.get(page_id)) |existing| {
                            // Refresh the cached page in-place.
                            existing.header = header;
                            @memcpy(existing.data, data[PageHeader.byteSize..]);
                            return existing;
                        }

                        return try self.cachePageFromWal(page_id, data);
                    } else |err| switch (err) {
                        error.WalPageNotFound => {},
                        else => {},
                    }
                }

                // Not in WAL — check the cache before hitting disk.
                if (self.pageCache.get(page_id)) |page| {
                    return page;
                }
            },
        }

        const file = if (self.file) |*f| f else return error.StorageUnavailable;
        const offset = BucketHeader.byteSize + (page_id * DEFAULT_PAGE_SIZE);

        var header_bytes: [PageHeader.byteSize]u8 = undefined;
        file.preadAll(header_bytes[0..], offset) catch |err| {
            return switch (err) {
                error.FileNotFound => PageError.PageNotFound,
                else => PageError.PageNotFound,
            };
        };
        const header = PageHeader.read(header_bytes[0..]);

        const page = try self.allocator.create(Page);
        page.* = try Page.init(self.allocator, self, header);

        if (page.header.page_id != page_id) {
            Page.deinit(page, self.allocator);
            self.allocator.destroy(page);
            return PageError.InvalidPageId;
        }

        file.preadAll(page.data, offset + PageHeader.byteSize) catch |err| {
            Page.deinit(page, self.allocator);
            self.allocator.destroy(page);
            return switch (err) {
                error.FileNotFound => PageError.PageNotFound,
                else => PageError.PageNotFound,
            };
        };

        if (try self.pageCache.put(page_id, page)) |evicted| {
            // Don't destroy evicted pages immediately - they may still be referenced
            // by B+ tree Node structures. They will be freed during pageCache.clear()
            _ = evicted;
        }

        return page;
    }

    /// Allocate a new Page from raw WAL data, cache it, and return it.
    fn cachePageFromWal(self: *Bucket, page_id: u64, data: [wal_mod.DEFAULT_PAGE_SIZE]u8) !*Page {
        const header = PageHeader.read(data[0..PageHeader.byteSize]);
        const page = try self.allocator.create(Page);
        page.* = try Page.init(self.allocator, self, header);
        @memcpy(page.data, data[PageHeader.byteSize..]);
        if (try self.pageCache.put(page_id, page)) |evicted| {
            _ = evicted;
        }
        return page;
    }

    /// Compare the SHM checkpoint_generation against our local snapshot.
    /// If another connection ran a checkpoint, flush the page cache and
    /// reload the BucketHeader from the main DB file so subsequent reads
    /// see the freshly-checkpointed data.
    fn maybeInvalidateCacheOnCheckpoint(self: *Bucket) void {
        const w = &(self.wal orelse return);
        const shm_gen = @atomicLoad(u64, &w.index.shmHeader().checkpoint_generation, .acquire);
        if (shm_gen == self.wal_checkpoint_generation) return;

        // Update generation FIRST to prevent re-entry from loadPage(0) below.
        self.wal_checkpoint_generation = shm_gen;

        // Another connection checkpointed — clear stale cached pages.
        self.pageCache.clear(self.allocator);
        self.cached_last_data_page = null;

        // Re-read the BucketHeader from the main DB file (now authoritative).
        if (self.file) |*f| {
            var hdr_bytes: [BucketHeader.byteSize]u8 = undefined;
            f.preadAll(hdr_bytes[0..], 0) catch {};
            self.header = BucketHeader.read(hdr_bytes[0..]);
        }

        // Reload indexes from the (possibly updated) meta page.
        self.resetLoadedIndexes();
        if (self.loadPage(0)) |meta| {
            self.loadIndices(meta) catch {};
        } else |_| {}
    }

    /// Return a nanosecond timestamp (as i64) for WAL frame ordering.
    /// Using nanoseconds instead of seconds avoids duplicate tx_timestamp
    /// values when multiple pages are written in the same second.
    inline fn walTimestamp() i64 {
        return @intCast(std.time.nanoTimestamp());
    }

    pub fn writePage(self: *Bucket, page: *const Page) !void {
        if (self.in_memory) {
            return;
        }

        if (self.active_tx != null) {
            return;
        }

        // Combine header + data into a single buffer.
        var buf: [DEFAULT_PAGE_SIZE]u8 = undefined;
        const header_bytes = PageHeader.write(&page.header);
        @memcpy(buf[0..PageHeader.byteSize], header_bytes[0..]);
        @memcpy(buf[PageHeader.byteSize..], page.data);

        // In WAL mode, write ONLY to the WAL — the main DB file stays
        // untouched until checkpoint. Other processes read new pages from
        // the WAL via the shared-memory index (MVCC).
        if (self.wal) |_| {
            // Defer the WAL append to commitWalTransaction() so that each
            // unique page is written to the WAL only once per logical
            // transaction, regardless of how many times the B+ tree
            // modified it during a single insert/delete call.
            // This mirrors SQLite behaviour: only the final committed page
            // state reaches the WAL, not every intermediate B+ tree write.
            try self.wal_pending_pages.put(page.header.page_id, {});
            return;
        }

        // Non-WAL mode: also defer to commitWalTransaction() (direct-file path)
        // so the same page is written to disk only once per logical transaction
        // regardless of how many intermediate B+ tree modifications touched it.
        if (!self.in_memory) {
            try self.wal_pending_pages.put(page.header.page_id, {});
            return;
        }
    }

    /// Called once per logical write transaction (insert / delete / buildIndex).
    /// Increments the write counter and fsyncs when the periodic threshold is reached.
    /// This avoids counting every B+ tree node write separately, which would cause
    /// fsync to fire ~5x more often than intended when an index is present.
    fn maybeSyncPeriodic(self: *Bucket) void {
        switch (self.write_durability) {
            .periodic => |threshold| {
                self.writes_since_sync += 1;
                if (self.writes_since_sync >= threshold) {
                    self.writes_since_sync = 0;
                    if (self.wal) |*w| {
                        w.sync() catch {};
                        return;
                    }
                    if (self.file) |*f| {
                        f.sync() catch {};
                    }
                }
            },
            else => {},
        }
    }

    /// Emit an oplog entry into the WAL shared-memory ring buffer.
    /// For insert/update, `payload` is the raw BSON bytes of the document.
    /// For delete, `payload` is null.
    /// Entries are only emitted when WAL mode is active and not in-memory.
    fn emitOplogEntry(self: *Bucket, op: WAL.OpKind, doc_id: u96, payload: ?[]const u8) void {
        if (self.in_memory) return;
        const w = &(self.wal orelse return);

        const ts = walTimestamp();

        // Determine payload kind and actual bytes to write.
        var payload_kind: WAL.PayloadKind = .none;
        var actual_payload: ?[]const u8 = null;

        if (payload) |p| {
            if (p.len <= WAL.OPLOG_INLINE_MAX) {
                payload_kind = .inline_doc;
                actual_payload = p;
            } else {
                // For large documents, store a DocRef so readers can
                // reconstruct the document from storage.  We don't have
                // the exact page_id/offset cheaply here, so we store
                // a zero ref — readers fall back to a full lookup by _id.
                payload_kind = .ref_loc;
                const ref = WAL.DocRef{ .page_id = 0, .offset = 0 };
                const ref_bytes: *const [WAL.DocRef.byte_size]u8 = @ptrCast(&ref);
                actual_payload = ref_bytes;
            }
        }

        w.index.acquireWrite();
        defer w.index.releaseWrite();
        const doc_id_bytes: [12]u8 = @bitCast(doc_id);
        w.index.appendOplogEntry(op, doc_id_bytes, ts, actual_payload, payload_kind);
    }

    /// Flush all pages dirtied during the current write transaction to the WAL,
    /// then optionally fsync.  Call this once at the end of insert() / delete() /
    /// buildIndex() instead of appending individual WAL frames inside writePage().
    ///
    /// Key invariant: the in-memory page cache already holds the final page state,
    /// so we just read each pending page_id from the cache and append it once.
    /// If a page was written multiple times (e.g., same B+ tree leaf touched on
    /// insert + split), only the most recent state is serialised — exactly the
    /// same as SQLite's per-transaction WAL write model.
    fn commitWalTransaction(self: *Bucket) void {
        if (self.in_memory) {
            self.wal_pending_pages.clearRetainingCapacity();
            self.wal_header_pending = false;
            return;
        }

        if (self.wal) |*w| {
            const tx_ts = walTimestamp();

            var it = self.wal_pending_pages.keyIterator();
            while (it.next()) |page_id_ptr| {
                const page_id = page_id_ptr.*;
                const page = self.pageCache.get(page_id) orelse continue;

                var buf: [DEFAULT_PAGE_SIZE]u8 = undefined;
                const hdr_bytes = PageHeader.write(&page.header);
                @memcpy(buf[0..PageHeader.byteSize], hdr_bytes[0..]);
                @memcpy(buf[PageHeader.byteSize..], page.data);

                w.appendPage(page_id, &buf, tx_ts) catch |err| {
                    if (err == error.WalIndexFailed) {
                        self.checkpointWal();
                        w.checkpoint() catch {};
                        w.appendPage(page_id, &buf, tx_ts) catch {};
                    }
                };
            }
            self.wal_pending_pages.clearRetainingCapacity();

            if (self.wal_header_pending) {
                // Embed the BucketHeader in the WAL header (v3+).
                const bytes = self.header.toBytes();
                @memcpy(&w.header.bucket_header, bytes[0..BucketHeader.byteSize]);
                self.wal_header_pending = false;

                // Publish page_count/doc_count to SHM so readers see
                // them immediately without any disk I/O.
                const shm = w.index.shmHeader();
                @atomicStore(u64, &shm.shm_page_count, self.header.page_count, .release);
                @atomicStore(u64, &shm.shm_doc_count, self.header.doc_count, .release);
            }

            if (self.write_durability == .all) {
                w.sync() catch {};
            } else {
                self.maybeSyncPeriodic();
            }

            // Auto-checkpoint: when the WAL grows past the threshold,
            // flush frames to the main DB file and truncate.
            if (self.wal_auto_checkpoint > 0 and w.live_frame_count >= self.wal_auto_checkpoint) {
                w.sync() catch {};
                self.checkpointWal();
                w.checkpoint() catch {};
                self.wal_checkpoint_generation = @atomicLoad(u64, &w.index.shmHeader().checkpoint_generation, .acquire);
            }

            return;
        }

        // No-WAL path: pwrite each staged page directly to the DB file.
        const file = if (self.file) |*f| f else {
            self.wal_pending_pages.clearRetainingCapacity();
            self.wal_header_pending = false;
            return;
        };

        var it = self.wal_pending_pages.keyIterator();
        while (it.next()) |page_id_ptr| {
            const page_id = page_id_ptr.*;
            const page = self.pageCache.get(page_id) orelse continue;

            var buf: [DEFAULT_PAGE_SIZE]u8 = undefined;
            const hdr_bytes = PageHeader.write(&page.header);
            @memcpy(buf[0..PageHeader.byteSize], hdr_bytes[0..]);
            @memcpy(buf[PageHeader.byteSize..], page.data);

            const offset = BucketHeader.byteSize + (page_id * DEFAULT_PAGE_SIZE);
            file.pwriteAll(buf[0..], offset) catch {};
        }
        self.wal_pending_pages.clearRetainingCapacity();

        if (self.wal_header_pending) {
            const bytes = self.header.toBytes();
            file.pwriteAll(bytes[0..], 0) catch {};
            self.wal_header_pending = false;
        }

        if (self.write_durability == .all) {
            file.sync() catch {};
            return;
        }
        self.maybeSyncPeriodic();
    }

    /// Force a sync of all pending writes to disk.
    /// Call this when you need guaranteed durability (e.g., after critical operations).
    pub fn flush(self: *Bucket) !void {
        try self.ensureNoActiveTransaction();
        if (self.in_memory) {
            return;
        }

        // Commit any pending WAL transaction (e.g. from a partial write) before syncing.
        self.commitWalTransaction();

        // Sync the WAL (if active) so all appended frames are durable.
        if (self.wal) |*w| {
            w.sync() catch {};
        }

        if (self.file) |*f| {
            try f.sync();
        }

        self.writes_since_sync = 0;
    }

    fn invalidateCachedPage(self: *Bucket, page_id: u64) void {
        if (self.pageCache.remove(page_id)) |old_page| {
            old_page.deinit(self.allocator);
            self.allocator.destroy(old_page);
        }
        if (self.cached_last_data_page) |last_page| {
            if (last_page.header.page_id == page_id) {
                self.cached_last_data_page = null;
            }
        }
    }

    fn resetLoadedIndexes(self: *Bucket) void {
        var idx_iter = self.indexes.iterator();
        while (idx_iter.next()) |pair| {
            const index_ptr = pair.value_ptr.*;
            const key = pair.key_ptr.*;
            self.allocator.destroy(index_ptr);
            self.allocator.free(key);
        }
        self.indexes.clearRetainingCapacity();
    }

    pub fn replicationCursor(self: *Bucket) !ReplicationCursor {
        if (self.in_memory or self.wal == null) return error.WalNotActive;
        return .{
            .generation = self.header.replication_generation,
            .next_frame_index = self.wal.?.header.frame_count,
        };
    }

    pub fn readReplicationBatch(self: *Bucket, from: ReplicationCursor, max_bytes: usize, allocator: std.mem.Allocator) !?[]u8 {
        if (self.in_memory or self.wal == null) return error.WalNotActive;

        const generation = self.header.replication_generation;
        if (from.generation < generation) return error.ReplicationGap;
        if (from.generation > generation) return error.InvalidCursor;

        const w = &(self.wal.?);
        const committed_frame_count = w.header.frame_count;
        if (from.next_frame_index > committed_frame_count) return error.InvalidCursor;
        if (from.next_frame_index == committed_frame_count) return null;

        if (max_bytes != 0 and max_bytes < ReplicationBatchHeader.byte_size + WAL.frame_size) {
            return error.InvalidArgument;
        }

        var frame_count = committed_frame_count - from.next_frame_index;
        if (max_bytes != 0) {
            const frame_capacity = (max_bytes - ReplicationBatchHeader.byte_size) / WAL.frame_size;
            if (frame_capacity == 0) return error.InvalidArgument;
            frame_count = @min(frame_count, @as(u64, @intCast(frame_capacity)));
        }

        const frame_bytes_len = try std.math.mul(usize, @as(usize, @intCast(frame_count)), WAL.frame_size);
        const total_size = ReplicationBatchHeader.byte_size + frame_bytes_len;
        const batch = try allocator.alloc(u8, total_size);
        errdefer allocator.free(batch);

        const header = ReplicationBatchHeader{
            .generation = generation,
            .start_frame_index = from.next_frame_index,
            .frame_count = frame_count,
            .wal_salt = w.header.salt,
            .latest_tx_timestamp = w.latest_committed_tx,
            .bucket_header = self.header.toBytes()[0..BucketHeader.byteSize].*,
        };
        const header_bytes = header.toBytes();
        @memcpy(batch[0..ReplicationBatchHeader.byte_size], &header_bytes);

        const wal_offset = w.data_offset + from.next_frame_index * @as(u64, WAL.frame_size);
        const n = (std.fs.File{ .handle = w.read_fd }).preadAll(batch[ReplicationBatchHeader.byte_size..], wal_offset) catch return error.WalReadFailed;
        if (n != frame_bytes_len) return error.WalReadFailed;

        return batch;
    }

    pub fn applyReplicationBatch(self: *Bucket, batch: []const u8) !ReplicationCursor {
        if (self.in_memory or self.wal == null) return error.WalNotActive;
        if (batch.len < ReplicationBatchHeader.v1_byte_size) return error.InvalidFormat;

        const batch_header = ReplicationBatchHeader.fromBytes(batch[0..@min(batch.len, ReplicationBatchHeader.byte_size)]);
        if (!std.mem.eql(u8, &batch_header.magic, &[_]u8{ 'A', 'R', 'P', 'L' })) return error.InvalidFormat;
        if (batch_header.version != 1 and batch_header.version != 2) return error.InvalidFormat;
        if (batch_header.page_size != DEFAULT_PAGE_SIZE or batch_header.page_size != @as(u16, @intCast(self.header.page_size))) return error.InvalidFormat;
        if (batch_header.frame_count == 0) return error.InvalidFormat;
        if (batch_header.generation != self.header.replication_generation) return error.ReplicationGap;

        const hdr_size: usize = if (batch_header.version >= 2) ReplicationBatchHeader.byte_size else ReplicationBatchHeader.v1_byte_size;
        const frame_bytes_len = try std.math.mul(usize, @as(usize, @intCast(batch_header.frame_count)), WAL.frame_size);
        const expected_size = hdr_size + frame_bytes_len;
        if (batch.len != expected_size) return error.InvalidFormat;

        const w = &(self.wal.?);
        if (w.header.frame_count == 0 and w.live_frame_count == 0 and w.header.salt != batch_header.wal_salt) {
            try w.adoptSalt(batch_header.wal_salt);
        }
        if (w.header.salt != batch_header.wal_salt) return error.InvalidFormat;

        const local_frame_count = w.header.frame_count;
        const incoming_end = batch_header.start_frame_index + batch_header.frame_count;
        const frame_bytes = batch[hdr_size..];

        if (batch_header.start_frame_index < local_frame_count) {
            if (incoming_end > local_frame_count) return error.ReplicationGap;

            const existing = try self.allocator.alloc(u8, frame_bytes.len);
            defer self.allocator.free(existing);
            const wal_offset = w.data_offset + batch_header.start_frame_index * @as(u64, WAL.frame_size);
            const n = (std.fs.File{ .handle = w.read_fd }).preadAll(existing, wal_offset) catch return error.WalReadFailed;
            if (n != existing.len) return error.WalReadFailed;
            if (!std.mem.eql(u8, existing, frame_bytes)) return error.ReplicationGap;
            return self.replicationCursor();
        }

        if (batch_header.start_frame_index != local_frame_count) return error.ReplicationGap;

        try w.appendCommittedFrames(frame_bytes, batch_header.latest_tx_timestamp);

        var meta_page_replicated = false;
        // v2 batch: use the embedded BucketHeader from the batch header.
        // v1 batch: look for HEADER_PAGE_ID sentinel frames (legacy).
        var replicated_header: ?BucketHeader = null;
        const zero: [64]u8 = [_]u8{0} ** 64;
        if (batch_header.version >= 2 and !std.mem.eql(u8, &batch_header.bucket_header, &zero)) {
            replicated_header = BucketHeader.read(&batch_header.bucket_header);
        }

        var i: u64 = 0;
        while (i < batch_header.frame_count) : (i += 1) {
            const frame_start = hdr_size + @as(usize, @intCast(i * WAL.frame_size));
            const frame = batch[frame_start .. frame_start + WAL.frame_size];
            const frame_header_bytes: *const [WAL.FrameHeader.byte_size]u8 = @ptrCast(frame[0..WAL.FrameHeader.byte_size].ptr);
            const frame_header = WAL.FrameHeader.fromBytes(frame_header_bytes);

            if (frame_header.page_id == WAL.HEADER_PAGE_ID) {
                // Legacy v1 sentinel frame.
                if (replicated_header == null) {
                    const page_data = frame[WAL.FrameHeader.byte_size..];
                    replicated_header = BucketHeader.read(page_data[0..BucketHeader.byteSize]);
                }
            } else {
                self.invalidateCachedPage(frame_header.page_id);
                if (frame_header.page_id == 0) {
                    meta_page_replicated = true;
                }
            }
        }

        if (replicated_header) |header| {
            self.header = header;
        }

        if (meta_page_replicated) {
            self.resetLoadedIndexes();
            const meta_page = try self.loadPage(0);
            try self.loadIndices(meta_page);
        }

        return self.replicationCursor();
    }

    pub fn dump(self: *Bucket, dest_path: []const u8) !void {
        self.rwlock.lock();
        defer self.rwlock.unlock();

        var out_file = try platform.openFile(self.allocator, dest_path, .{
            .read = true,
            .write = true,
            .create = true,
            .truncate = true,
        });
        defer out_file.close();

        const header_bytes = self.header.toBytes();
        try out_file.pwriteAll(header_bytes[0..], 0);

        var page_id: u64 = 0;
        while (page_id < self.header.page_count) : (page_id += 1) {
            const page = try self.loadPage(page_id);
            const page_offset = BucketHeader.byteSize + (page.header.page_id * DEFAULT_PAGE_SIZE);
            const page_header_bytes = PageHeader.write(&page.header);
            try out_file.pwriteAll(page_header_bytes[0..], page_offset);
            try out_file.pwriteAll(page.data, page_offset + PageHeader.byteSize);
        }

        try out_file.sync();
    }

    pub fn createNewPage(self: *Bucket, page_type: PageType) !*Page {
        // For now, we'll just use the page_id as a sequential number
        // In a real implementation, you'd want to track available page IDs
        const new_page_id = self.header.page_count;
        self.header.page_count += 1;
        try self.flushHeader(); // Ensure the header is flushed to disk
        const page = try self.allocator.create(Page);
        page.* = try Page.init(self.allocator, self, PageHeader.init(page_type, new_page_id));
        if (self.active_tx) |tx| {
            try tx.pending_pages.put(new_page_id, page);
            return page;
        }
        if (try self.pageCache.put(new_page_id, page)) |evicted| {
            // Don't destroy evicted pages immediately - they may still be referenced
            // by B+ tree Node structures. They will be freed during pageCache.clear()
            _ = evicted;
        }
        return page;
    }

    pub const DocHeader = packed struct { // 16bytes
        doc_id: u96,
        is_deleted: u8,
        reserved: u24,
        pub const byteSize = 16;

        pub fn fromMemory(memory: []const u8) error{ InvalidDocId, InvalidHeader }!DocHeader {
            const header = DocHeader{
                .is_deleted = memory[12],
                .doc_id = std.mem.readInt(u96, memory[0..12], .little),
                .reserved = std.mem.readInt(u24, memory[13..16], .little),
            };
            if (header.doc_id == 0) {
                return error.InvalidDocId;
            }
            if (header.reserved != 0) {
                // std.debug.print("Header is correpted: {any} \n", .{header});
                // @breakpoint();
                return error.InvalidHeader;
            }
            return header;
        }

        pub fn write(self: DocHeader, writer: *std.Io.Writer) !void {
            // Write the header to the writer
            try writer.writeInt(u96, self.doc_id, .little);
            try writer.writeInt(u8, self.is_deleted, .little);
            try writer.writeInt(u24, self.reserved, .little);
        }

        pub fn read(reader: anytype) !DocHeader {
            // Read the header from the reader
            const header = DocHeader{
                .doc_id = reader.readInt(u96, .little),
                .is_deleted = reader.readInt(u8, .little),
                .reserved = reader.readInt(u24, .little),
            };
            if (header.doc_id == 0) {
                return error.InvalidDocId;
            }
            return header;
        }
    };

    const DocInsertResult = struct {
        doc_id: u96,
        page_id: u64,
        offset: u16,
    };

    fn findLastDataPage(self: *Bucket) !?*Page {
        // Return cached page if still valid (not freed, correct type)
        if (self.cached_last_data_page) |cached| {
            if (self.active_tx) |tx| {
                if (tx.pending_pages.get(cached.header.page_id)) |pending| {
                    if (pending.header.page_type == PageType.Data) {
                        self.cached_last_data_page = pending;
                        return pending;
                    }
                }
            }
            if (cached.header.page_type == PageType.Data) {
                return cached;
            }
        }

        // Find the last data page
        var pageIter = PageIterator{
            .bucket = self,
            .index = self.header.page_count,
            .type = PageType.Data,
            .reverse = true,
        };
        while (try pageIter.next()) |page| {
            if (page.header.page_type == PageType.Data) {
                self.cached_last_data_page = page;
                return page;
            }
        }
        return null;
    }

    pub const InsertError = error{
        DuplicateKey,
    };

    pub fn insert(self: *Bucket, insertable: bson.BSONDocument) !DocInsertResult {
        try self.ensureNoActiveTransaction();
        self.rwlock.lock();
        defer self.rwlock.unlock();
        return self.insertLocked(insertable, true);
    }

    fn insertLocked(self: *Bucket, insertable: bson.BSONDocument, autocommit: bool) !DocInsertResult {
        var doc = insertable;
        const docId = self.objectIdGenerator.next();
        const needCleanup = doc.get("_id") == null;
        if (needCleanup) {
            // If the document doesn't have an _id, generate one
            doc = try doc.set(self.allocator, "_id", BSONValue.init(docId));
        }
        defer {
            if (needCleanup) {
                doc.deinit(self.allocator);
            }
        }

        // Use the document's existing buffer directly
        const doc_size = doc.buffer.len;
        const encoded_doc = doc.buffer;
        var page: *Page = undefined;

        var planned_index_inserts = std.ArrayList(PlannedIndexInsert){};
        defer {
            for (planned_index_inserts.items) |*plan| {
                plan.values.deinit(self.allocator);
            }
            planned_index_inserts.deinit(self.allocator);
        }

        var idx_iter = self.indexes.iterator();
        while (idx_iter.next()) |entry| {
            var values = std.ArrayList(BSONValue){};
            var retain_values = false;
            defer {
                if (!retain_values) {
                    values.deinit(self.allocator);
                }
            }

            const index_ptr = entry.value_ptr.*;
            const path = entry.key_ptr.*;
            self.bindIndex(index_ptr);

            const has_values = try self.gatherIndexValuesForPath(&doc, path, index_ptr.options, &values);
            if (!has_values) {
                continue;
            }

            try planned_index_inserts.append(self.allocator, PlannedIndexInsert{
                .index = index_ptr,
                .values = values,
            });
            retain_values = true;
        }

        // Preflight unique indexes before writing any document bytes.
        // This avoids partial writes when duplicate key errors occur.
        for (planned_index_inserts.items) |plan| {
            if (plan.index.options.unique != 1) continue;

            for (plan.values.items, 0..) |value, value_idx| {
                var prior_idx: usize = 0;
                while (prior_idx < value_idx) : (prior_idx += 1) {
                    if (plan.values.items[prior_idx].eql(&value)) {
                        return error.DuplicateKey;
                    }
                }

                self.bindIndex(plan.index);
                if (try plan.index.hasValue(value)) {
                    return error.DuplicateKey;
                }
            }
        }

        // If no pages exist yet, create one
        if (try findLastDataPage(self)) |p| {
            page = try self.loadPageForWrite(p.header.page_id);
        } else {
            page = try self.createNewPage(.Data);
            self.cached_last_data_page = page;
            try self.writePage(page);
        }

        // Check if the page has enough space for header and doc size
        if ((page.data.len - page.header.used_size) <= (4 + DocHeader.byteSize)) { // 20 bytes
            // Not enough space, create a new page
            page = try self.createNewPage(.Data);
            self.cached_last_data_page = page;
            try self.writePage(page);
        }

        // std.debug.print("\nDecided to write on page {d}, offset: {d}\n", .{ page.header.page_id, page.header.used_size });

        // Write the document header
        const doc_header = DocHeader{
            .is_deleted = 0,
            .doc_id = docId.toInt(),
            .reserved = 0,
        };
        const result = DocInsertResult{
            .doc_id = doc_header.doc_id,
            .page_id = page.header.page_id,
            .offset = page.header.used_size,
        };

        // If this page doesn't yet have a recorded first readable byte,
        // set it to the offset where we're about to write the document.
        // This marks the start of readable data for the page (useful for
        // future scans/compaction).
        if (page.header.first_readable_byte == 0) {
            @branchHint(.unlikely);
            page.header.first_readable_byte = result.offset;
        }

        var offset = &page.header.used_size;
        @memcpy(page.data[offset.* .. offset.* + @sizeOf(DocHeader)], &std.mem.toBytes(doc_header));
        offset.* += @intCast(DocHeader.byteSize);

        var bytes_written: usize = 0;
        const bytes_to_write: usize = doc_size;

        while (bytes_written < bytes_to_write) {
            offset = &page.header.used_size;
            // Check if current page has enough space
            const available_space = page.data.len - offset.*;
            const writable_buffer = page.data[offset.*..];

            // Determine how much to write
            const to_write = @min(bytes_to_write - bytes_written, available_space);
            // std.debug.print("Writing ({d}){d} bytes to the page \n", .{ page.header.used_size, to_write });
            @memcpy(
                writable_buffer[0..to_write],
                encoded_doc[bytes_written .. bytes_written + to_write],
            );
            // Insert document at the current position
            offset.* += @intCast(to_write);
            bytes_written += to_write;

            // Write updated page back to disk
            try self.writePage(page);

            // If there are still bytes left to write, create a new page
            if (bytes_written < bytes_to_write) {
                page = try self.createNewPage(.Data);
                self.cached_last_data_page = page;
                try self.writePage(page);
            }
        }

        const index_location = Index.DocumentLocation{
            .pageId = result.page_id,
            .offset = result.offset,
        };

        for (planned_index_inserts.items) |plan| {
            self.bindIndex(plan.index);
            for (plan.values.items) |value| {
                try plan.index.insertWithOptions(value, index_location, .{ .skip_uniqueness_check = true });
            }
        }

        self.header.doc_count += 1;
        try self.flushHeader();

        if (autocommit) {
            // Commit WAL transaction: flush each staged page to WAL once and
            // optionally fsync. This is the correct place to do it — all in-memory
            // B+ tree modifications are complete and the page cache holds final state.
            self.commitWalTransaction();
        }

        // Emit oplog entry for the insert (unless suppressed by transform).
        if (!self._suppress_oplog) {
            try self.stageOplogEntry(.insert, doc_header.doc_id, encoded_doc);
        }

        // After writing is done, free the page resources
        return result;
    }

    const DocumentMeta = struct {
        page_id: u64,
        offset: u16,
        header: DocHeader,
    };

    const DocumentLocation = struct {
        page_id: u64,
        offset: u16,
    };

    const IteratorResult = struct {
        page_id: u64,
        offset: u16,
        header: DocHeader,
        data: []u8,
    };

    const ListRecord = struct {
        doc: BSONDocument,
        page_id: u64,
        offset: u16,
        doc_id: ObjectId,
    };

    const PlannedIndexInsert = struct {
        index: *Index,
        values: std.ArrayList(BSONValue) = .{},
    };

    const IndexValueError = error{
        IndexedStringTooLong,
    };

    fn docLocationKey(loc: Index.DocumentLocation) u128 {
        return (@as(u128, loc.pageId) << 16) | @as(u128, loc.offset);
    }

    fn isIndexableValue(value: bson.BSONValue) bool {
        return switch (value) {
            .document, .binary => false,
            .array => false,
            .string => value.string.value.len <= MAX_INDEX_STRING_BYTES,
            else => true,
        };
    }

    fn appendSingleIndexValue(
        values: *std.ArrayList(BSONValue),
        allocator: mem.Allocator,
        value: bson.BSONValue,
    ) (IndexValueError || mem.Allocator.Error)!bool {
        switch (value) {
            .document, .binary => return false,
            .array => return false,
            .string => {
                if (value.string.value.len > MAX_INDEX_STRING_BYTES) {
                    return IndexValueError.IndexedStringTooLong;
                }
                try values.append(allocator, value);
                return true;
            },
            else => {
                try values.append(allocator, value);
                return true;
            },
        }
    }

    fn collectIndexableValue(
        values: *std.ArrayListUnmanaged(BSONValue),
        allocator: mem.Allocator,
        value: bson.BSONValue,
    ) (IndexValueError || mem.Allocator.Error)!usize {
        switch (value) {
            .array => |arr| {
                var iter = arr.iter();
                var count: usize = 0;
                while (iter.next()) |pair| {
                    if (try appendSingleIndexValue(values, allocator, pair.value)) {
                        count += 1;
                    }
                }
                return count;
            },
            else => {
                if (try appendSingleIndexValue(values, allocator, value)) {
                    return 1;
                }
                return 0;
            },
        }
    }

    fn gatherIndexValuesForPath(
        self: *Bucket,
        doc: *const bson.BSONDocument,
        path: []const u8,
        options: IndexOptions,
        values: *std.ArrayListUnmanaged(BSONValue),
    ) (IndexValueError || mem.Allocator.Error)!bool {
        var appended: usize = 0;
        if (doc.getPath(path)) |value| {
            appended = try collectIndexableValue(values, self.allocator, value);
            if (appended > 0) {
                return true;
            }
        }

        if (options.sparse == 1) {
            return false;
        }

        try values.append(self.allocator, BSONValue{ .null = bson.BSONNull{} });
        return true;
    }

    fn cloneIndexValue(
        self: *Bucket,
        value: BSONValue,
        owned_strings: *std.ArrayList([]u8),
    ) mem.Allocator.Error!BSONValue {
        return switch (value) {
            .string => {
                const dup = try self.allocator.dupe(u8, value.string.value);
                errdefer self.allocator.free(dup);
                try owned_strings.append(self.allocator, dup);
                return BSONValue{ .string = .{ .value = dup } };
            },
            else => value,
        };
    }

    fn bindIndex(self: *Bucket, index: *Index) void {
        if (index.bucket != self) {
            index.bucket = self;
        }
    }

    pub const ScanIterator = struct {
        bucket: *Bucket,
        page: *Page,
        allocator: std.mem.Allocator,
        initialized: bool = false,
        offset: u16 = 0,
        pageIterator: PageIterator,
        readDeleted: bool = false,

        fn init(bucket: *Bucket, allocator: std.mem.Allocator) !ScanIterator {
            return .{
                .bucket = bucket,
                .page = undefined,
                .allocator = allocator,
                .pageIterator = .{
                    .bucket = bucket,
                    .type = .Data,
                },
            };
        }

        pub fn deinit(self: *ScanIterator) void {
            _ = self; // No cleanup needed when using arena allocator
        }

        pub fn step(self: *ScanIterator) !?DocumentMeta {
            if (!self.initialized) {
                self.initialized = true;
                self.page = try self.pageIterator.next() orelse {
                    // No data pages yet.  Reset so the next call retries
                    // (WAL mode: another connection may create pages later).
                    self.initialized = false;
                    return null;
                };
                // std.debug.print("List iterator initialized, page {d} loaded\n", .{self.page.header.page_id});
            }

            if (self.offset >= (@as(u16, @truncate(self.page.data.len)) - 20)) {
                // Check if we need to load a new page
                self.page = try self.pageIterator.next() orelse return null;
                self.offset = 0;
            }

            const header = std.mem.bytesToValue(DocHeader, self.page.data[self.offset .. self.offset + @sizeOf(DocHeader)]);
            if (header.reserved != 0 or header.is_deleted > 1) {
                // std.log.err("Doc header is corrupted:\nAt page: {d} position: {x} header looks like: {x}\n", .{ self.page.header.page_id, self.offset, self.page.data[self.offset .. self.offset + @sizeOf(DocHeader)] });
                // @panic("Header is corrupted");
                // std.log.err("page data: {x}\n", .{self.page.data[0..128]});

                return error.InvalidHeader;
            }

            // std.debug.print("Page approved, header {x}\n", .{header.reserved});

            if (header.doc_id == 0) {
                // WAL mode: the writer may have appended documents to
                // this page since we last loaded it.  Reload via
                // loadPage (which refreshes the cached page in-place
                // from the WAL) and re-check.
                if (self.bucket.wal != null) {
                    _ = self.bucket.loadPage(self.page.header.page_id) catch return null;
                    const refreshed = std.mem.bytesToValue(DocHeader, self.page.data[self.offset .. self.offset + @sizeOf(DocHeader)]);
                    if (refreshed.doc_id != 0 and refreshed.reserved == 0 and refreshed.is_deleted <= 1) {
                        self.offset += @sizeOf(DocHeader);
                        return .{
                            .page_id = self.page.header.page_id,
                            .offset = self.offset - @sizeOf(DocHeader),
                            .header = refreshed,
                        };
                    }
                }
                return null; // No more documents
            }
            self.offset += @sizeOf(DocHeader);

            return .{
                .page_id = self.page.header.page_id,
                .offset = self.offset - @sizeOf(DocHeader),
                .header = header,
            };
        }
        pub fn next(self: *ScanIterator) !?IteratorResult {
            // Check if we have reached the end of the page

            var location = try self.step() orelse return null;
            // var header = location.header;

            while (location.header.is_deleted == 1 and !self.readDeleted) {
                // Skip deleted documents without reading their data
                const docOffset = self.page.data[self.offset..];
                const doc_len = mem.readInt(u32, docOffset[0..4], .little);
                const availableToSkip: u16 = @as(u16, @truncate(self.page.data.len)) - self.offset;

                if (doc_len <= availableToSkip) {
                    // Deleted doc fits in current page, just advance offset
                    self.offset += @as(u16, @truncate(doc_len));
                } else {
                    // Deleted doc spans multiple pages
                    // Calculate how many pages to skip without loading them
                    var remaining: u32 = doc_len - availableToSkip;
                    const pageDataLen: u32 = @intCast(self.page.data.len);

                    // Skip full pages
                    while (remaining > pageDataLen) {
                        self.page = try self.pageIterator.next() orelse return null;
                        remaining -= pageDataLen;
                    }

                    // Load final page and set offset
                    if (remaining > 0) {
                        self.page = try self.pageIterator.next() orelse return null;
                        self.offset = @as(u16, @truncate(remaining));
                    } else {
                        self.offset = 0;
                    }
                }

                location = try self.step() orelse return null;
            }

            const doc_len = mem.readInt(u32, self.page.data[self.offset..][0..4], .little);
            var leftToCopy: u32 = doc_len;
            var availableToCopy: u16 = @truncate(@min(doc_len, self.page.data.len - self.offset));
            if (availableToCopy == doc_len) {
                // Document fits entirely in current page - return a slice directly
                const docSlice = self.page.data[self.offset .. self.offset + doc_len];
                self.offset += @truncate(doc_len);
                return .{
                    .page_id = location.page_id,
                    .offset = location.offset,
                    .header = location.header,
                    .data = docSlice,
                };
            }

            // Allocate fresh buffer from arena for this document
            const docBuffer = try self.allocator.alloc(u8, doc_len);

            var writableBuffer = docBuffer[0..doc_len];
            @memcpy(writableBuffer[0..availableToCopy], self.page.data[self.offset .. self.offset + availableToCopy]);
            self.offset += @truncate(availableToCopy);
            leftToCopy -= availableToCopy;

            while (leftToCopy > 0) {
                writableBuffer = docBuffer[(doc_len - leftToCopy)..doc_len];
                // Check if we need to load a new page
                self.page = try self.pageIterator.next() orelse return null;
                availableToCopy = @truncate(@min(leftToCopy, self.page.data.len));

                @memcpy(writableBuffer[0..availableToCopy], self.page.data[0..availableToCopy]);
                self.offset = availableToCopy;
                leftToCopy -= availableToCopy;
            }

            // Return the arena-allocated buffer directly
            // The buffer will be freed when the arena is freed
            return .{
                .page_id = location.page_id,
                .offset = location.offset,
                .header = location.header,
                .data = docBuffer,
            };
        }
    };

    const QueryPlan = struct {
        const Source = enum {
            full_scan,
            index,
        };

        const IndexStrategy = enum {
            range,
            points,
            /// All branches of a `$or` filter are covered by indexes.
            /// `logical_branches[0..logical_branch_count]` holds one index + bounds per branch.
            or_union,
            /// All branches of a `$nor` filter are covered by indexes, and results
            /// are produced by scanning the canonical `_id` index while excluding
            /// any document locations seen in the branch scans.
            nor_exclusion,
        };

        const IndexBounds = struct {
            lower: ?Index.RangeBound = null,
            upper: ?Index.RangeBound = null,
        };

        /// Per-branch plan used by logical union/exclusion strategies.
        const BranchPlan = struct {
            index: *Index,
            bounds: IndexBounds,
        };

        const MAX_LOGICAL_BRANCHES = 32;

        source: Source = .full_scan,
        index: ?*Index = null,
        filter_index: ?usize = null,
        index_path: ?[]const u8 = null,
        bounds: IndexBounds = .{},
        eager: bool = false,
        sort_covered: bool = false,
        index_strategy: IndexStrategy = .range,
        point_values: ?bson.BSONValue = null,
        logical_branches: [MAX_LOGICAL_BRANCHES]BranchPlan = undefined,
        logical_branch_count: u8 = 0,
    };

    inline fn planUsesPointStrategy(plan: *const QueryPlan) bool {
        return plan.index_strategy == .points and plan.point_values != null;
    }

    inline fn planMatchesRange(plan: *const QueryPlan, index_ptr: *Index, path: []const u8) bool {
        return plan.source == .index and plan.index_strategy == .range and plan.index != null and plan.index.? == index_ptr and plan.index_path != null and mem.eql(u8, plan.index_path.?, path);
    }

    inline fn tightenLowerBound(current: *?Index.RangeBound, candidate: Index.RangeBound) void {
        if (current.* == null) {
            current.* = candidate;
            return;
        }
        const existing = current.*.?;
        switch (existing.value.order(candidate.value)) {
            .lt => current.* = candidate,
            .gt => {},
            .eq => {
                if (existing.filter == .gte and candidate.filter == .gt) {
                    current.* = candidate;
                }
            },
        }
    }

    inline fn tightenUpperBound(current: *?Index.RangeBound, candidate: Index.RangeBound) void {
        if (current.* == null) {
            current.* = candidate;
            return;
        }
        const existing = current.*.?;
        switch (existing.value.order(candidate.value)) {
            .gt => current.* = candidate,
            .lt => {},
            .eq => {
                if (existing.filter == .lte and candidate.filter == .lt) {
                    current.* = candidate;
                }
            },
        }
    }

    const PlannedLeaf = struct {
        score: u8,
        index: *Index,
        path: []const u8,
        bounds: QueryPlan.IndexBounds = .{},
        strategy: QueryPlan.IndexStrategy,
        point_values: ?bson.BSONValue = null,
    };

    fn planLeafFilter(self: *Bucket, filter: query.Filter) ?PlannedLeaf {
        return switch (filter) {
            .eq => |data| blk: {
                const index_ptr = self.indexes.get(data.path) orelse break :blk null;
                break :blk .{
                    .score = 100,
                    .index = index_ptr,
                    .path = data.path,
                    .bounds = .{
                        .lower = Index.RangeBound.gte(data.value),
                        .upper = Index.RangeBound.lte(data.value),
                    },
                    .strategy = .range,
                };
            },
            .lt => |data| blk: {
                const index_ptr = self.indexes.get(data.path) orelse break :blk null;
                break :blk .{
                    .score = 80,
                    .index = index_ptr,
                    .path = data.path,
                    .bounds = .{ .upper = Index.RangeBound.lt(data.value) },
                    .strategy = .range,
                };
            },
            .lte => |data| blk: {
                const index_ptr = self.indexes.get(data.path) orelse break :blk null;
                break :blk .{
                    .score = 80,
                    .index = index_ptr,
                    .path = data.path,
                    .bounds = .{ .upper = Index.RangeBound.lte(data.value) },
                    .strategy = .range,
                };
            },
            .gt => |data| blk: {
                const index_ptr = self.indexes.get(data.path) orelse break :blk null;
                break :blk .{
                    .score = 80,
                    .index = index_ptr,
                    .path = data.path,
                    .bounds = .{ .lower = Index.RangeBound.gt(data.value) },
                    .strategy = .range,
                };
            },
            .gte => |data| blk: {
                const index_ptr = self.indexes.get(data.path) orelse break :blk null;
                break :blk .{
                    .score = 80,
                    .index = index_ptr,
                    .path = data.path,
                    .bounds = .{ .lower = Index.RangeBound.gte(data.value) },
                    .strategy = .range,
                };
            },
            .between => |data| blk: {
                const lower = data.value.array.get("0") orelse break :blk null;
                const upper = data.value.array.get("1") orelse break :blk null;
                if (!isIndexableValue(lower) or !isIndexableValue(upper)) break :blk null;
                const index_ptr = self.indexes.get(data.path) orelse break :blk null;
                break :blk .{
                    .score = 85,
                    .index = index_ptr,
                    .path = data.path,
                    .bounds = .{
                        .lower = Index.RangeBound.gt(lower),
                        .upper = Index.RangeBound.lt(upper),
                    },
                    .strategy = .range,
                };
            },
            .in => |data| blk: {
                if (data.value != bson.BSONValueType.array) break :blk null;
                var iter = data.value.array.iter();
                var count: usize = 0;
                while (iter.next()) |pair| {
                    if (!isIndexableValue(pair.value)) break :blk null;
                    count += 1;
                }
                if (count == 0) break :blk null;
                const index_ptr = self.indexes.get(data.path) orelse break :blk null;
                break :blk .{
                    .score = 95,
                    .index = index_ptr,
                    .path = data.path,
                    .strategy = .points,
                    .point_values = data.value,
                };
            },
            .startsWith => |data| blk: {
                if (data.value != .string or !isIndexableValue(data.value)) break :blk null;
                const index_ptr = self.indexes.get(data.path) orelse break :blk null;
                break :blk .{
                    .score = 70,
                    .index = index_ptr,
                    .path = data.path,
                    .bounds = .{ .lower = Index.RangeBound.gte(data.value) },
                    .strategy = .range,
                };
            },
            else => null,
        };
    }

    fn applyPlannedLeaf(plan: *QueryPlan, best_score: *u8, filter_index: usize, candidate: PlannedLeaf) void {
        const matches_current = candidate.strategy == .range and planMatchesRange(plan, candidate.index, candidate.path);
        if (candidate.score > best_score.*) {
            best_score.* = candidate.score;
            plan.source = .index;
            plan.index = candidate.index;
            plan.filter_index = filter_index;
            plan.index_path = candidate.path;
            plan.bounds = candidate.bounds;
            plan.sort_covered = false;
            plan.index_strategy = candidate.strategy;
            plan.point_values = candidate.point_values;
            plan.logical_branch_count = 0;
        } else if (matches_current) {
            if (candidate.bounds.lower) |lower| {
                tightenLowerBound(&plan.bounds.lower, lower);
            }
            if (candidate.bounds.upper) |upper| {
                tightenUpperBound(&plan.bounds.upper, upper);
            }
        }
    }

    fn planBestBranchFilter(self: *Bucket, group: query.FilterGroup) ?QueryPlan.BranchPlan {
        var best_score: u8 = 0;
        var best: ?QueryPlan.BranchPlan = null;

        for (group) |filter| {
            switch (filter) {
                .@"and" => |groups| {
                    for (groups) |subgroup| {
                        if (self.planBestBranchFilter(subgroup)) |branch_plan| {
                            const score = if (branch_plan.bounds.lower != null and branch_plan.bounds.upper != null) @as(u8, 100) else @as(u8, 80);
                            if (score > best_score) {
                                best_score = score;
                                best = branch_plan;
                            }
                        }
                    }
                },
                .@"or", .nor => {},
                else => {
                    const candidate = self.planLeafFilter(filter) orelse continue;
                    if (candidate.score > best_score) {
                        best_score = candidate.score;
                        best = .{
                            .index = candidate.index,
                            .bounds = candidate.bounds,
                        };
                    }
                },
            }
        }
        return best;
    }

    fn considerFilterForPlan(self: *Bucket, filter: query.Filter, filter_index: usize, best_score: *u8, plan: *QueryPlan) void {
        switch (filter) {
            .@"and" => |groups| {
                for (groups) |group| {
                    for (group) |subfilter| {
                        self.considerFilterForPlan(subfilter, filter_index, best_score, plan);
                    }
                }
            },
            .@"or" => |groups| {
                if (groups.len == 0 or groups.len > QueryPlan.MAX_LOGICAL_BRANCHES) return;
                var branch_plans: [QueryPlan.MAX_LOGICAL_BRANCHES]QueryPlan.BranchPlan = undefined;
                for (groups, 0..) |group, branch_idx| {
                    const bp = self.planBestBranchFilter(group) orelse return;
                    branch_plans[branch_idx] = bp;
                }
                const score: u8 = 60;
                if (score > best_score.*) {
                    best_score.* = score;
                    plan.source = .index;
                    plan.index = null;
                    plan.filter_index = filter_index;
                    plan.index_path = null;
                    plan.bounds = .{};
                    plan.sort_covered = false;
                    plan.index_strategy = .or_union;
                    plan.point_values = null;
                    plan.logical_branch_count = @intCast(groups.len);
                    for (0..groups.len) |i| plan.logical_branches[i] = branch_plans[i];
                }
            },
            .nor => |groups| {
                if (groups.len == 0 or groups.len > QueryPlan.MAX_LOGICAL_BRANCHES) return;
                var branch_plans: [QueryPlan.MAX_LOGICAL_BRANCHES]QueryPlan.BranchPlan = undefined;
                for (groups, 0..) |group, branch_idx| {
                    const bp = self.planBestBranchFilter(group) orelse return;
                    branch_plans[branch_idx] = bp;
                }
                const score: u8 = 50;
                if (score > best_score.*) {
                    best_score.* = score;
                    plan.source = .index;
                    plan.index = null;
                    plan.filter_index = filter_index;
                    plan.index_path = null;
                    plan.bounds = .{};
                    plan.sort_covered = false;
                    plan.index_strategy = .nor_exclusion;
                    plan.point_values = null;
                    plan.logical_branch_count = @intCast(groups.len);
                    for (0..groups.len) |i| plan.logical_branches[i] = branch_plans[i];
                }
            },
            else => {
                const candidate = self.planLeafFilter(filter) orelse return;
                applyPlannedLeaf(plan, best_score, filter_index, candidate);
            },
        }
    }

    pub const ListIterator = struct {
        docList: []BSONDocument = &[_]BSONDocument{},
        arena: *std.heap.ArenaAllocator,
        ally: std.mem.Allocator,
        bucket: *Bucket,
        query: query.Query,
        plan: QueryPlan,
        index: usize = 0,

        scanner: ScanIterator,
        index_iterator: Index.RangeIterator = undefined,
        index_iterator_initialized: bool = false,
        point_iterator: Index.RangeIterator = undefined,
        point_iterator_initialized: bool = false,
        point_values: std.ArrayListUnmanaged(bson.BSONValue) = .{},
        point_values_consumed: usize = 0,
        point_value_total: usize = 0,
        // Optimize for common case: single-point queries don't need deduplication
        // Strategy: none = no dedup needed, last = check last location, full = use hashmap
        point_dedup_strategy: enum { none, check_last, use_hashmap } = .none,
        point_last_location: u128 = 0, // For check_last strategy
        point_seen_set: std.AutoHashMap(u128, void) = undefined,
        point_seen_initialized: bool = false,
        // $or union index scan state
        or_iterators: []Index.RangeIterator = &[_]Index.RangeIterator{},
        or_iterator_idx: usize = 0,
        or_seen_set: std.AutoHashMap(u128, void) = undefined,
        or_seen_initialized: bool = false,
        limitLeft: ?u64 = null,
        offsetLeft: u64 = 0,
        last_emitted: ?query.CursorAnchor = null,
        cursor_mode_enabled: bool = false,
        next: *const fn (*ListIterator) error{ OutOfMemory, ScanError }!?BSONDocument = nextUnfetched,

        fn ensureIndexIterator(self: *ListIterator) error{ScanError}!*Index.RangeIterator {
            if (Bucket.planUsesPointStrategy(&self.plan)) {
                return error.ScanError;
            }
            if (!self.index_iterator_initialized) {
                self.index_iterator = self.bucket.initIndexIterator(&self.plan) catch {
                    return error.ScanError;
                };
                self.index_iterator_initialized = true;
            }
            return &self.index_iterator;
        }

        pub fn prequery(self: *ListIterator) !void {
            const ally = self.arena.allocator();
            var docList: std.ArrayList(BSONDocument) = .{};
            try self.bucket.collectDocs(&docList, ally, &self.query, &self.plan);

            var resultSlice = docList.items;
            if (self.query.sortConfig) |sortConfig| {
                if (!self.plan.sort_covered) {
                    std.mem.sort(BSONDocument, resultSlice, sortConfig, query.Query.sort);
                }
            }

            const offset = if (self.query.sector) |sector| sector.offset orelse 0 else 0;
            const limit = if (self.query.sector) |sector| sector.limit orelse resultSlice.len else resultSlice.len;

            self.docList = resultSlice[@min(resultSlice.len, offset)..@min(offset + limit, resultSlice.len)];
            self.index = 0;
        }

        fn nextPrefetched(self: *ListIterator) error{ OutOfMemory, ScanError }!?BSONDocument {
            if (self.index >= self.docList.len) {
                return null;
            }
            const doc = self.docList[self.index];
            self.index += 1;
            return doc;
        }

        fn rememberRecord(self: *ListIterator, record: *const ListRecord) void {
            const user_id = record.doc.get("_id") orelse return;
            self.last_emitted = .{
                .doc_id = record.doc_id,
                .user_id = user_id,
                .page_id = record.page_id,
                .offset = record.offset,
            };
        }

        fn nextFullScanRecord(self: *ListIterator) error{ OutOfMemory, ScanError }!?ListRecord {
            std.debug.assert(self.plan.source == .full_scan);

            while (true) {
                if (self.limitLeft != null and self.limitLeft.? == 0) {
                    return null;
                }

                const doc = self.scanner.next() catch |err| switch (err) {
                    error.OutOfMemory => return error.OutOfMemory,
                    else => return error.ScanError,
                } orelse return null;

                const bsonDoc: BSONDocument = .{ .buffer = doc.data };
                if (self.query.match(&bsonDoc) and self.offsetLeft == 0) {
                    if (self.limitLeft != null) self.limitLeft = self.limitLeft.? - 1;
                    return .{
                        .doc = bsonDoc,
                        .page_id = doc.page_id,
                        .offset = doc.offset,
                        .doc_id = ObjectId.fromInt(doc.header.doc_id),
                    };
                }

                if (self.offsetLeft != 0) {
                    self.offsetLeft -= 1;
                }
            }
        }

        pub fn nextUnfetched(self: *ListIterator) error{ OutOfMemory, ScanError }!?BSONDocument {
            const record = try self.nextFullScanRecord() orelse return null;
            self.rememberRecord(&record);
            return record.doc;
        }

        fn nextIndexRangeRecord(self: *ListIterator) error{ OutOfMemory, ScanError }!?ListRecord {
            if (self.limitLeft != null and self.limitLeft.? == 0) {
                return null;
            }
            const ally = self.ally;
            const iterator = try self.ensureIndexIterator();

            while (true) {
                const loc = iterator.next() catch |err| switch (err) {
                    error.OutOfMemory => return error.OutOfMemory,
                    else => return error.ScanError,
                } orelse return null;
                const header = self.bucket.readDocHeaderAt(.{
                    .page_id = loc.pageId,
                    .offset = loc.offset,
                }) catch |err| switch (err) {
                    error.OutOfMemory => return error.OutOfMemory,
                    error.DocumentDeleted => continue,
                    else => return error.ScanError,
                };

                var doc = self.bucket.readDocAt(ally, .{
                    .page_id = loc.pageId,
                    .offset = loc.offset,
                }) catch |err| switch (err) {
                    error.OutOfMemory => return error.OutOfMemory,
                    error.DocumentDeleted => continue,
                    else => return error.ScanError,
                };

                if (!(self.query.filters.len == 0 or self.query.match(&doc))) {
                    continue;
                }

                if (self.offsetLeft != 0) {
                    self.offsetLeft -= 1;
                    continue;
                }

                if (self.limitLeft) |*limit| {
                    if (limit.* == 0) {
                        return null;
                    }
                    limit.* -= 1;
                }

                return .{
                    .doc = doc,
                    .page_id = loc.pageId,
                    .offset = loc.offset,
                    .doc_id = ObjectId.fromInt(header.doc_id),
                };
            }
        }

        fn nextPointIndexRecord(self: *ListIterator) error{ OutOfMemory, ScanError }!?ListRecord {
            if (self.plan.index == null) {
                return null;
            }

            if (self.point_value_total == 0) {
                return null;
            }

            const ally = self.ally;

            // Initialize deduplication mechanism based on strategy
            if (!self.point_seen_initialized and self.point_dedup_strategy == .use_hashmap) {
                self.point_seen_set = std.AutoHashMap(u128, void).init(ally);
                self.point_seen_initialized = true;
            }

            while (true) {
                if (!self.point_iterator_initialized) {
                    const next_value = self.nextPointFilterValue() orelse return null;
                    const index_ptr = self.plan.index.?;
                    // self.bucket.bindIndex(index_ptr);
                    self.point_iterator = index_ptr.point(next_value) catch return error.ScanError;
                    self.point_iterator_initialized = true;
                }

                const maybe_loc = self.point_iterator.next() catch |err| switch (err) {
                    error.OutOfMemory => return error.OutOfMemory,
                    else => return error.ScanError,
                };
                const loc = maybe_loc orelse {
                    self.point_iterator_initialized = false;
                    continue;
                };

                const key = docLocationKey(loc);

                // Apply deduplication based on chosen strategy
                switch (self.point_dedup_strategy) {
                    .none => {}, // Single point query - no duplicates possible
                    .check_last => {
                        // Two-value query - just check if same as last location
                        if (key == self.point_last_location) {
                            continue;
                        }
                        self.point_last_location = key;
                    },
                    .use_hashmap => {
                        // Multiple values - use full hashmap deduplication
                        if (self.point_seen_set.contains(key)) {
                            continue;
                        }
                        self.point_seen_set.put(key, {}) catch return error.OutOfMemory;
                    },
                }

                var doc = self.bucket.readDocAt(ally, .{
                    .page_id = loc.pageId,
                    .offset = loc.offset,
                }) catch |err| switch (err) {
                    error.OutOfMemory => return error.OutOfMemory,
                    error.DocumentDeleted => continue,
                    else => return error.ScanError,
                };

                if (!(self.query.filters.len == 0 or self.query.match(&doc))) {
                    // ally.free(doc.buffer);
                    continue;
                }

                if (self.offsetLeft != 0) {
                    self.offsetLeft -= 1;
                    // ally.free(doc.buffer);
                    continue;
                }

                if (self.limitLeft) |*limit| {
                    if (limit.* == 0) {
                        // ally.free(doc.buffer);
                        return null;
                    }
                    limit.* -= 1;
                }

                const header = self.bucket.readDocHeaderAt(.{
                    .page_id = loc.pageId,
                    .offset = loc.offset,
                }) catch |err| switch (err) {
                    error.OutOfMemory => return error.OutOfMemory,
                    error.DocumentDeleted => continue,
                    else => return error.ScanError,
                };

                return .{
                    .doc = doc,
                    .page_id = loc.pageId,
                    .offset = loc.offset,
                    .doc_id = ObjectId.fromInt(header.doc_id),
                };
            }
        }

        fn cursorAnchorMatchesDoc(anchor: *const query.CursorAnchor, doc: *const BSONDocument) bool {
            const user_id = doc.get("_id") orelse return false;
            return user_id.eql(&anchor.user_id);
        }

        fn nextOrUnionRecord(self: *ListIterator) error{ OutOfMemory, ScanError }!?ListRecord {
            if (self.limitLeft != null and self.limitLeft.? == 0) return null;
            const ally = self.ally;

            if (!self.or_seen_initialized) {
                self.or_seen_set = std.AutoHashMap(u128, void).init(ally);
                self.or_seen_initialized = true;
            }

            while (self.or_iterator_idx < self.or_iterators.len) {
                const iter = &self.or_iterators[self.or_iterator_idx];
                const maybe_loc = iter.next() catch |err| switch (err) {
                    error.OutOfMemory => return error.OutOfMemory,
                    else => return error.ScanError,
                };
                const loc = maybe_loc orelse {
                    // This branch is exhausted; move to the next.
                    self.or_iterator_idx += 1;
                    continue;
                };

                const key = docLocationKey(loc);
                if (self.or_seen_set.contains(key)) continue;
                try self.or_seen_set.put(key, {});

                var doc = self.bucket.readDocAt(ally, .{
                    .page_id = loc.pageId,
                    .offset = loc.offset,
                }) catch |err| switch (err) {
                    error.OutOfMemory => return error.OutOfMemory,
                    error.DocumentDeleted => continue,
                    else => return error.ScanError,
                };

                if (!(self.query.filters.len == 0 or self.query.match(&doc))) continue;

                if (self.offsetLeft != 0) {
                    self.offsetLeft -= 1;
                    continue;
                }

                if (self.limitLeft) |*limit| {
                    if (limit.* == 0) return null;
                    limit.* -= 1;
                }

                const header = self.bucket.readDocHeaderAt(.{
                    .page_id = loc.pageId,
                    .offset = loc.offset,
                }) catch |err| switch (err) {
                    error.OutOfMemory => return error.OutOfMemory,
                    error.DocumentDeleted => continue,
                    else => return error.ScanError,
                };

                return .{
                    .doc = doc,
                    .page_id = loc.pageId,
                    .offset = loc.offset,
                    .doc_id = ObjectId.fromInt(header.doc_id),
                };
            }
            return null;
        }

        fn resumeFullScanFromPhysicalAnchor(self: *ListIterator, anchor: *const query.CursorAnchor) error{ OutOfMemory, ScanError }!bool {
            const header = self.bucket.readDocHeaderAt(.{
                .page_id = anchor.page_id,
                .offset = anchor.offset,
            }) catch |err| switch (err) {
                error.OutOfMemory => return error.OutOfMemory,
                else => return false,
            };
            if (header.doc_id != anchor.doc_id.toInt()) return false;

            self.scanner = try ScanIterator.init(self.bucket, self.ally);
            self.scanner.page = self.bucket.loadPage(anchor.page_id) catch |err| switch (err) {
                error.OutOfMemory => return error.OutOfMemory,
                else => return error.ScanError,
            };
            self.scanner.initialized = true;
            self.scanner.offset = anchor.offset;
            self.scanner.pageIterator.index = anchor.page_id + 1;

            const record = try self.nextFullScanRecord() orelse return false;
            if (!cursorAnchorMatchesDoc(anchor, &record.doc)) {
                return false;
            }
            return true;
        }

        fn skipPastCursorAnchor(self: *ListIterator) error{ OutOfMemory, ScanError, InvalidCursor }!void {
            const cursor = self.query.cursor orelse return;
            const anchor = cursor.anchor orelse return;

            if (self.plan.source == .full_scan) {
                if (try self.resumeFullScanFromPhysicalAnchor(&anchor)) {
                    return;
                }
                self.scanner = try ScanIterator.init(self.bucket, self.ally);
                while (try self.nextFullScanRecord()) |record| {
                    if (cursorAnchorMatchesDoc(&anchor, &record.doc)) {
                        return;
                    }
                }
                return error.InvalidCursor;
            }

            const use_points = Bucket.planUsesPointStrategy(&self.plan);
            std.debug.assert(!use_points);
            while (try self.nextIndexRangeRecord()) |record| {
                if (cursorAnchorMatchesDoc(&anchor, &record.doc)) {
                    return;
                }
            }
            return error.InvalidCursor;
        }

        pub fn exportCursor(self: *ListIterator, allocator: std.mem.Allocator) error{ OutOfMemory, UnsupportedCursorQuery }!BSONDocument {
            if (self.plan.eager or self.query.sortConfig != null or self.query.sector != null) {
                return error.UnsupportedCursorQuery;
            }

            var builder = bson.Builder.init(allocator) catch |err| switch (err) {
                error.OutOfMemory => return error.OutOfMemory,
                else => unreachable,
            };
            defer builder.deinit();
            var root = builder.docEncoder();
            root.putValue("version", .{ .int32 = .{ .value = 1 } }) catch |err| switch (err) {
                error.OutOfMemory => return error.OutOfMemory,
                else => unreachable,
            };

            const mode_value: []const u8 = switch (self.plan.source) {
                .full_scan => "full_scan",
                .index => if (Bucket.planUsesPointStrategy(&self.plan))
                    return error.UnsupportedCursorQuery
                else if (self.plan.index_strategy == .or_union or self.plan.index_strategy == .nor_exclusion)
                    return error.UnsupportedCursorQuery
                else
                    "index_range",
            };
            root.putValue("mode", .{ .string = .{ .value = mode_value } }) catch |err| switch (err) {
                error.OutOfMemory => return error.OutOfMemory,
                else => unreachable,
            };

            if (self.plan.source == .index) {
                const index_path = self.plan.index_path orelse return error.UnsupportedCursorQuery;
                root.putValue("indexPath", BSONValue.init(index_path)) catch |err| switch (err) {
                    error.OutOfMemory => return error.OutOfMemory,
                    else => unreachable,
                };
            }

            if (self.last_emitted) |anchor| {
                var anchor_doc = root.object("anchor") catch |err| switch (err) {
                    error.OutOfMemory => return error.OutOfMemory,
                    else => unreachable,
                };
                anchor_doc.putValue("docId", BSONValue.init(anchor.doc_id)) catch |err| switch (err) {
                    error.OutOfMemory => return error.OutOfMemory,
                    else => unreachable,
                };
                anchor_doc.putValue("_id", anchor.user_id) catch |err| switch (err) {
                    error.OutOfMemory => return error.OutOfMemory,
                    else => unreachable,
                };
                anchor_doc.putValue("pageId", .{ .int64 = .{ .value = @intCast(anchor.page_id) } }) catch |err| switch (err) {
                    error.OutOfMemory => return error.OutOfMemory,
                    else => unreachable,
                };
                anchor_doc.putValue("offset", .{ .int32 = .{ .value = @intCast(anchor.offset) } }) catch |err| switch (err) {
                    error.OutOfMemory => return error.OutOfMemory,
                    else => unreachable,
                };
                anchor_doc.end() catch unreachable;
            }

            return builder.finish() catch |err| switch (err) {
                error.OutOfMemory => return error.OutOfMemory,
                else => unreachable,
            };
        }

        fn nextIndex(self: *ListIterator) error{ OutOfMemory, ScanError }!?BSONDocument {
            std.debug.assert(self.plan.source == .index);
            const use_points = Bucket.planUsesPointStrategy(&self.plan);
            const record = if (use_points)
                try self.nextPointIndexRecord()
            else if (self.plan.index_strategy == .or_union)
                try self.nextOrUnionRecord()
            else
                try self.nextIndexRangeRecord();
            const final_record = record orelse return null;
            self.rememberRecord(&final_record);
            return final_record.doc;
        }

        fn nextPointFilterValue(self: *ListIterator) ?bson.BSONValue {
            if (self.point_values_consumed >= self.point_value_total) return null;
            const value = self.point_values.items[self.point_values_consumed];
            self.point_values_consumed += 1;
            return value;
        }

        pub fn deinit(self: *ListIterator) !void {
            if (self.plan.source == .full_scan) {
                self.scanner.deinit();
            }
            if (self.point_seen_initialized) {
                self.point_seen_set.deinit();
            }
            if (self.point_value_total > 0) {
                self.point_values.deinit(self.arena.allocator());
            }
            if (self.or_seen_initialized) {
                self.or_seen_set.deinit();
            }
        }
    };

    fn planQuery(self: *Bucket, q: *const query.Query) QueryPlan {
        var plan = QueryPlan{};
        var best_score: u8 = 0;

        for (q.filters, 0..) |filter, idx| {
            self.considerFilterForPlan(filter, idx, &best_score, &plan);
        }

        if (q.sortConfig) |sortConfig| {
            const sort_path = switch (sortConfig) {
                .asc => |p| p,
                .desc => |p| p,
            };

            if (plan.source == .index and plan.index_strategy == .range) {
                if (plan.index_path) |path| {
                    if (std.mem.eql(u8, path, sort_path)) {
                        if (plan.index) |idx_ptr| {
                            plan.sort_covered = switch (sortConfig) {
                                .asc => idx_ptr.options.reverse == 0,
                                .desc => idx_ptr.options.reverse == 1,
                            };
                        }
                    }
                }
            } else if (best_score == 0) {
                if (self.indexes.get(sort_path)) |index_ptr| {
                    plan.source = .index;
                    plan.index = index_ptr;
                    plan.filter_index = null;
                    plan.index_path = sort_path;
                    plan.bounds = .{};
                    plan.sort_covered = switch (sortConfig) {
                        .asc => index_ptr.options.reverse == 0,
                        .desc => index_ptr.options.reverse == 1,
                    };
                    plan.index_strategy = .range;
                }
            }

            plan.eager = !plan.sort_covered;
        } else {
            plan.eager = false;
        }

        if (plan.source == .index and plan.index_strategy == .nor_exclusion) {
            plan.eager = true;
            plan.sort_covered = false;
        }

        return plan;
    }

    const PlanningError = error{
        InvalidIndexPlan,
        InvalidLowerBoundOperator,
        InvalidUpperBoundOperator,
    };

    fn initIndexIterator(self: *Bucket, plan: *const QueryPlan) PlanningError!Index.RangeIterator {
        const index_ptr = plan.index orelse return PlanningError.InvalidIndexPlan;
        self.bindIndex(index_ptr);
        return index_ptr.range(plan.bounds.lower, plan.bounds.upper) catch |err| switch (err) {
            error.InvalidLowerBoundOperator => return PlanningError.InvalidLowerBoundOperator,
            error.InvalidUpperBoundOperator => return PlanningError.InvalidUpperBoundOperator,
            else => return PlanningError.InvalidIndexPlan,
        };
    }

    fn releaseIndexedDocBuffer(self: *Bucket, ally: std.mem.Allocator, loc: DocumentLocation, doc: BSONDocument) void {
        const page = self.loadPage(loc.page_id) catch {
            ally.free(doc.buffer);
            return;
        };
        const doc_start = @intFromPtr(doc.buffer.ptr);
        const page_start = @intFromPtr(page.data.ptr);
        const page_end = page_start + page.data.len;
        if (doc_start >= page_start and doc_start < page_end) return;
        ally.free(doc.buffer);
    }

    fn collectExcludedDocIds(
        self: *Bucket,
        plan: *const QueryPlan,
        excluded: *std.AutoHashMap(u128, void),
    ) error{ OutOfMemory, ScanError }!void {
        for (0..plan.logical_branch_count) |i| {
            const bp = &plan.logical_branches[i];
            self.bindIndex(bp.index);
            var branch_iter = bp.index.range(bp.bounds.lower, bp.bounds.upper) catch return error.ScanError;
            while (true) {
                const maybe_loc = branch_iter.next() catch |err| switch (err) {
                    error.OutOfMemory => return error.OutOfMemory,
                    else => return error.ScanError,
                };
                const loc = maybe_loc orelse break;
                const header = self.readDocHeaderAt(.{
                    .page_id = loc.pageId,
                    .offset = loc.offset,
                }) catch |err| switch (err) {
                    error.OutOfMemory => return error.OutOfMemory,
                    error.DocumentDeleted => continue,
                    else => return error.ScanError,
                };
                try excluded.put(header.doc_id, {});
            }
        }
    }

    fn validateCursorQuery(self: *Bucket, q: *const query.Query, plan: *const QueryPlan) error{ InvalidCursor, UnsupportedCursorQuery }!void {
        _ = self;
        const cursor = q.cursor orelse return;

        if (q.sortConfig != null or q.sector != null or plan.eager) {
            return error.UnsupportedCursorQuery;
        }

        if (plan.source == .index and plan.index_strategy == .points) {
            return error.UnsupportedCursorQuery;
        }

        if (plan.source == .index and (plan.index_strategy == .or_union or plan.index_strategy == .nor_exclusion)) {
            return error.UnsupportedCursorQuery;
        }

        switch (cursor.mode) {
            .full_scan => {
                if (plan.source != .full_scan) return error.InvalidCursor;
            },
            .index_range => {
                if (plan.source != .index or plan.index_strategy != .range) return error.InvalidCursor;
                const plan_index_path = plan.index_path orelse return error.InvalidCursor;
                const cursor_index_path = cursor.index_path orelse return error.InvalidCursor;
                if (!mem.eql(u8, plan_index_path, cursor_index_path)) {
                    return error.InvalidCursor;
                }
            },
        }
    }

    fn collectDocs(
        self: *Bucket,
        docList: *std.ArrayList(BSONDocument),
        ally: std.mem.Allocator,
        q: *const query.Query,
        plan: *const QueryPlan,
    ) error{ OutOfMemory, ScanError }!void {
        switch (plan.source) {
            .full_scan => {
                var iterator = ScanIterator.init(self, ally) catch |err| switch (err) {
                    error.OutOfMemory => return error.OutOfMemory,
                    else => return error.ScanError,
                };
                defer iterator.deinit();
                while (true) {
                    const next_doc = iterator.next() catch |err| switch (err) {
                        error.OutOfMemory => return error.OutOfMemory,
                        else => return error.ScanError,
                    } orelse break;
                    const docRaw = next_doc;
                    const doc = BSONDocument{ .buffer = docRaw.data };
                    if (q.filters.len == 0 or q.match(&doc)) {
                        try docList.append(ally, doc);
                    } else {
                        self.releaseIndexedDocBuffer(ally, .{
                            .page_id = docRaw.page_id,
                            .offset = docRaw.offset,
                        }, doc);
                    }
                }
            },
            .index => {
                if (plan.index_strategy == .or_union) {
                    var seen = std.AutoHashMap(u128, void).init(ally);
                    defer seen.deinit();
                    for (0..plan.logical_branch_count) |i| {
                        const bp = &plan.logical_branches[i];
                        self.bindIndex(bp.index);
                        var iter = bp.index.range(bp.bounds.lower, bp.bounds.upper) catch return error.ScanError;
                        while (true) {
                            const maybe_loc = iter.next() catch |err| switch (err) {
                                error.OutOfMemory => return error.OutOfMemory,
                                else => return error.ScanError,
                            };
                            const loc = maybe_loc orelse break;
                            const key = docLocationKey(loc);
                            if (seen.contains(key)) continue;
                            try seen.put(key, {});
                            var doc = self.readDocAt(ally, .{
                                .page_id = loc.pageId,
                                .offset = loc.offset,
                            }) catch |err| switch (err) {
                                error.OutOfMemory => return error.OutOfMemory,
                                error.DocumentDeleted => continue,
                                else => return error.ScanError,
                            };
                            if (q.filters.len == 0 or q.match(&doc)) {
                                try docList.append(ally, doc);
                            } else {
                                self.releaseIndexedDocBuffer(ally, .{
                                    .page_id = loc.pageId,
                                    .offset = loc.offset,
                                }, doc);
                            }
                        }
                    }
                    return;
                }

                if (plan.index_strategy == .nor_exclusion) {
                    var excluded = std.AutoHashMap(u128, void).init(ally);
                    defer excluded.deinit();
                    try self.collectExcludedDocIds(plan, &excluded);

                    var iterator = ScanIterator.init(self, ally) catch |err| switch (err) {
                        error.OutOfMemory => return error.OutOfMemory,
                        else => return error.ScanError,
                    };
                    defer iterator.deinit();
                    while (true) {
                        const next_doc = iterator.next() catch |err| switch (err) {
                            error.OutOfMemory => return error.OutOfMemory,
                            else => return error.ScanError,
                        };
                        const doc_raw = next_doc orelse break;
                        if (excluded.contains(doc_raw.header.doc_id)) {
                            self.releaseIndexedDocBuffer(ally, .{
                                .page_id = doc_raw.page_id,
                                .offset = doc_raw.offset,
                            }, .{ .buffer = doc_raw.data });
                            continue;
                        }
                        const doc: BSONDocument = .{ .buffer = doc_raw.data };

                        if (q.filters.len == 0 or q.match(&doc)) {
                            try docList.append(ally, doc);
                        } else {
                            self.releaseIndexedDocBuffer(ally, .{
                                .page_id = doc_raw.page_id,
                                .offset = doc_raw.offset,
                            }, doc);
                        }
                    }
                    return;
                }

                const index_ptr = plan.index orelse return error.ScanError;
                self.bindIndex(index_ptr);

                if (planUsesPointStrategy(plan)) {
                    const in_values = plan.point_values orelse return error.ScanError;

                    // Pre-size the deduplication map based on expected number of documents
                    var seen = std.AutoHashMap(u128, void).init(ally);
                    defer seen.deinit();

                    // Pre-allocate capacity to reduce rehashing
                    var value_count: usize = 0;
                    var count_iter = in_values.array.iter();
                    while (count_iter.next()) |pair| {
                        if (isIndexableValue(pair.value)) value_count += 1;
                    }
                    try seen.ensureTotalCapacity(@intCast(value_count * 4)); // Estimate 4 docs per value

                    var value_iter = in_values.array.iter();
                    while (value_iter.next()) |pair| {
                        if (!isIndexableValue(pair.value)) continue;

                        var range_iter = index_ptr.point(pair.value) catch return error.ScanError;
                        while (true) {
                            const maybe_loc = range_iter.next() catch |err| switch (err) {
                                error.OutOfMemory => return error.OutOfMemory,
                                else => return error.ScanError,
                            };
                            const loc = maybe_loc orelse break;
                            const key = docLocationKey(loc);
                            if (seen.contains(key)) continue;
                            try seen.put(key, {});

                            var doc = self.readDocAt(ally, .{
                                .page_id = loc.pageId,
                                .offset = loc.offset,
                            }) catch |err| switch (err) {
                                error.OutOfMemory => return error.OutOfMemory,
                                error.DocumentDeleted => continue,
                                else => return error.ScanError,
                            };

                            if (q.filters.len == 0 or q.match(&doc)) {
                                try docList.append(ally, doc);
                            } else {
                                self.releaseIndexedDocBuffer(ally, .{
                                    .page_id = loc.pageId,
                                    .offset = loc.offset,
                                }, doc);
                            }
                        }
                    }
                } else {
                    var iterator = self.initIndexIterator(plan) catch return error.ScanError;
                    while (true) {
                        const maybe_loc = iterator.next() catch |err| switch (err) {
                            error.OutOfMemory => return error.OutOfMemory,
                            else => return error.ScanError,
                        };
                        const loc = maybe_loc orelse break;
                        var doc = self.readDocAt(ally, .{
                            .page_id = loc.pageId,
                            .offset = loc.offset,
                        }) catch |err| switch (err) {
                            error.OutOfMemory => return error.OutOfMemory,
                            error.DocumentDeleted => continue,
                            else => return error.ScanError,
                        };
                        if (q.filters.len == 0 or q.match(&doc)) {
                            try docList.append(ally, doc);
                        } else {
                            self.releaseIndexedDocBuffer(ally, .{
                                .page_id = loc.pageId,
                                .offset = loc.offset,
                            }, doc);
                        }
                    }
                }
            },
        }
    }

    // ── Oplog Subscription ────────────────────────────────────────────

    pub const SubscriptionError = error{
        /// The subscriber fell behind and the ring buffer has wrapped past
        /// unread entries. The caller must re-subscribe from a fresh query.
        OplogGap,
        OutOfMemory,
        WalNotActive,
    };

    /// An operation envelope returned by the subscription poll API.
    pub const ChangeEvent = struct {
        /// Monotonically increasing sequence number.
        seqno: u64,
        /// The kind of mutation (insert, update, delete).
        op: WAL.OpKind,
        /// Raw bytes of the _id field (ObjectId as 12 bytes).
        doc_id: [12]u8,
        /// Wall-clock timestamp of the operation (nanoseconds).
        timestamp: i64,
        /// The document body for insert/update, or null for delete.
        document: ?BSONDocument,

        /// Serialize this event into a BSON document.
        /// The caller owns the returned document and must free it.
        fn toBson(self: *const ChangeEvent, a: mem.Allocator) !BSONDocument {
            var pairs_buf: [5]bson.BSONKeyValuePair = undefined;
            var n: usize = 0;

            pairs_buf[n] = .{ .key = "seqno", .value = .{ .int64 = .{ .value = @bitCast(self.seqno) } } };
            n += 1;
            const op_str: []const u8 = switch (self.op) {
                .insert => "insert",
                .update => "update",
                .delete => "delete",
            };
            pairs_buf[n] = .{ .key = "op", .value = .{ .string = .{ .value = op_str } } };
            n += 1;
            pairs_buf[n] = .{ .key = "doc_id", .value = .{ .objectId = .{ .value = .{ .buffer = self.doc_id } } } };
            n += 1;
            pairs_buf[n] = .{ .key = "ts", .value = .{ .int64 = .{ .value = self.timestamp } } };
            n += 1;

            if (self.document) |doc| {
                pairs_buf[n] = .{ .key = "doc", .value = .{ .document = doc } };
                n += 1;
            }

            return try BSONDocument.fromPairs(a, pairs_buf[0..n]);
        }
    };
    const seqno_type = usize;

    /// Reader-side subscription handle.  Created via `Bucket.subscribe()`,
    /// the caller polls `poll()` from their event loop to drain matching
    /// oplog entries in batches.
    pub const Subscription = struct {
        bucket: *Bucket,
        q: query.Query,
        /// Sequence number of the last consumed oplog entry.
        last_seqno: u64,
        /// Ring position corresponding to the byte after last_seqno's entry.
        ring_pos: u32,
        /// Scratch buffer for reading payloads out of the ring.
        payload_buf: [WAL.OPLOG_INLINE_MAX]u8,
        /// Materialized BSON batch from the last poll. Freed on the next
        /// poll or on deinit. Callers must consume the document before the
        /// next poll call — see API docs.
        last_batch_buf: ?[]const u8 = null,

        /// Return the latest committed oplog seqno visible to readers.
        pub fn currentSeqno(self: *Subscription) u64 {
            const w = &(self.bucket.wal orelse return 0);
            return @atomicLoad(seqno_type, @as(*seqno_type, @ptrCast(&w.index.shmHeader().oplog_seqno)), .acquire);
        }

        /// Poll for the next batch of change events that match the stored
        /// query.  Returns a BSON document `{batch: [...events]}` when at
        /// least one matching event is available, or `null` when there is
        /// nothing new.
        ///
        /// **Lifetime:** the returned document is backed by memory owned by
        /// the Subscription.  It remains valid until the next `poll()` call
        /// or until `deinit()` — whichever comes first.
        ///
        /// Returns `SubscriptionError.OplogGap` if the ring has wrapped
        /// past our read position.
        pub fn poll(self: *Subscription, max_events: u32) SubscriptionError!?BSONDocument {
            // Free previous batch.
            if (self.last_batch_buf) |buf| {
                self.bucket.allocator.free(buf);
                self.last_batch_buf = null;
            }

            const w = &(self.bucket.wal orelse return error.WalNotActive);
            const shm = w.index.shmHeader();

            const head_seqno: u64 = @atomicLoad(seqno_type, @as(*seqno_type, @ptrCast(&shm.oplog_seqno)), .acquire);
            if (head_seqno < self.last_seqno) {
                // On 32-bit builds, seqno_type is usize (u32) so the published
                // SHM watermark can wrap. Treat regression as a gap so callers
                // re-subscribe instead of stalling forever.
                return error.OplogGap;
            }
            if (head_seqno == self.last_seqno) {
                return null;
            }

            // Check for gap: has the ring evicted entries we haven't read yet?
            const oldest: u64 = @atomicLoad(seqno_type, @as(*seqno_type, @ptrCast(&shm.oplog_oldest_seqno)), .acquire);
            // Guard: if the writer stored oldest before seqno (transient
            // race), oldest > head — just wait for the next poll.
            if (oldest > head_seqno) {
                return null;
            }
            if (oldest > 0 and self.last_seqno > 0 and oldest > self.last_seqno + 1) {
                return error.OplogGap;
            }

            const a = self.bucket.allocator;

            // Collect matching events.
            var event_docs = std.ArrayList(BSONDocument){};
            defer {
                for (event_docs.items) |edoc| edoc.deinit(a);
                event_docs.deinit(a);
            }

            var pos = self.ring_pos;
            while (event_docs.items.len < max_events) {
                const entry = w.index.readOplogEntry(pos, &self.payload_buf) orelse break;
                if (entry.header.seqno <= self.last_seqno) break;
                if (entry.header.seqno == 0) break; // uninit

                var maybe_doc: ?BSONDocument = null;
                if (entry.header.payload_kind == .inline_doc and entry.payload.len > 0) {
                    maybe_doc = BSONDocument.init(entry.payload);
                }

                const dominated = if (maybe_doc) |*doc| self.q.match(doc) else true;
                if (dominated) {
                    const ev = ChangeEvent{
                        .seqno = entry.header.seqno,
                        .op = entry.header.op,
                        .doc_id = entry.header.doc_id,
                        .timestamp = entry.header.timestamp,
                        .document = maybe_doc,
                    };
                    const ev_doc = ev.toBson(a) catch return error.OutOfMemory;
                    event_docs.append(a, ev_doc) catch return error.OutOfMemory;
                }

                self.last_seqno = entry.header.seqno;
                pos = entry.next_pos;
            }
            self.ring_pos = pos;

            if (event_docs.items.len == 0) return null;

            // Build BSON array from collected event docs.
            const arr_pairs = a.alloc(bson.BSONKeyValuePair, event_docs.items.len) catch return error.OutOfMemory;
            defer a.free(arr_pairs);

            const idx_keys = a.alloc([20]u8, event_docs.items.len) catch return error.OutOfMemory;
            defer a.free(idx_keys);

            for (event_docs.items, 0..) |edoc, i| {
                const key = std.fmt.bufPrint(&idx_keys[i], "{d}", .{i}) catch unreachable;
                arr_pairs[i] = .{ .key = key, .value = .{ .document = edoc } };
            }

            var arr_doc = BSONDocument.fromPairs(a, arr_pairs) catch return error.OutOfMemory;
            defer arr_doc.deinit(a);

            var root_pair = [_]bson.BSONKeyValuePair{
                .{ .key = "batch", .value = .{ .array = arr_doc } },
            };
            var result_doc = BSONDocument.fromPairs(a, &root_pair) catch return error.OutOfMemory;

            // Transfer ownership to subscription.
            self.last_batch_buf = result_doc.buffer;
            // Prevent the local from freeing the buffer.
            result_doc = BSONDocument.init(self.last_batch_buf.?);

            return BSONDocument.init(self.last_batch_buf.?);
        }

        pub fn deinit(self: *Subscription) void {
            if (self.last_batch_buf) |buf| {
                self.bucket.allocator.free(buf);
            }
            self.q.deinit(self.bucket.allocator);
            self.bucket.allocator.destroy(self);
        }
    };

    /// Create a subscription on this bucket. The subscription will deliver
    /// change events (as operation envelopes) matching the provided query.
    /// The caller should poll `sub.poll()` from their event loop.
    /// Requires WAL mode to be active.
    pub fn subscribe(self: *Bucket, q: query.Query) SubscriptionError!*Subscription {
        if (self.wal == null) return error.WalNotActive;
        const w = &(self.wal.?);

        // Snapshot current seqno so the reader doesn't receive historical events.
        const initial_seqno: u64 = @atomicLoad(seqno_type, @as(*seqno_type, @ptrCast(&w.index.shmHeader().oplog_seqno)), .acquire);
        const initial_pos = @atomicLoad(u32, &w.index.shmHeader().oplog_write_pos, .acquire);

        const sub = self.allocator.create(Subscription) catch return error.OutOfMemory;
        sub.* = .{
            .bucket = self,
            .q = q,
            .last_seqno = initial_seqno,
            .ring_pos = initial_pos,
            .payload_buf = [_]u8{0} ** WAL.OPLOG_INLINE_MAX,
        };
        return sub;
    }

    pub fn listIterate(self: *Bucket, arena: *std.heap.ArenaAllocator, q: query.Query) !*ListIterator {
        var ally = arena.allocator();
        const plan = self.planQuery(&q);
        try self.validateCursorQuery(&q, &plan);

        var scanner_instance: ScanIterator = undefined;
        if (plan.source == .full_scan) {
            scanner_instance = try ScanIterator.init(self, ally);
        }

        const rc = try ally.create(ListIterator);
        rc.* = ListIterator{
            .bucket = self,
            .arena = arena,
            .ally = ally,
            .query = q,
            .plan = plan,
            .docList = &[_]BSONDocument{},
            .scanner = if (plan.source == .full_scan) scanner_instance else undefined,
            .index_iterator = undefined,
            .index_iterator_initialized = false,
            .point_iterator = undefined,
            .point_iterator_initialized = false,
            .point_values = .{},
            .point_values_consumed = 0,
            .point_value_total = 0,
            .point_dedup_strategy = .none,
            .point_last_location = 0,
            .point_seen_set = undefined,
            .point_seen_initialized = false,
            .or_iterators = &[_]Index.RangeIterator{},
            .or_iterator_idx = 0,
            .or_seen_set = undefined,
            .or_seen_initialized = false,
            .limitLeft = null,
            .offsetLeft = 0,
            .index = 0,
            .last_emitted = null,
            .cursor_mode_enabled = q.cursor != null,
        };

        if (Bucket.planUsesPointStrategy(&rc.plan)) {
            const point_values = rc.plan.point_values orelse return error.ScanError;
            var iter = point_values.array.iter();
            while (iter.next()) |pair| {
                if (!isIndexableValue(pair.value)) continue;
                try rc.point_values.append(ally, pair.value);
            }
            rc.point_value_total = rc.point_values.items.len;
            rc.point_values_consumed = 0;

            if (rc.point_value_total == 1) {
                rc.point_dedup_strategy = .none;
            } else if (rc.point_value_total == 2) {
                rc.point_dedup_strategy = .check_last;
            } else {
                rc.point_dedup_strategy = .use_hashmap;
            }
        }

        if (rc.plan.index_strategy == .or_union) {
            // Allocate one range iterator per $or branch.
            const branch_count = rc.plan.logical_branch_count;
            rc.or_iterators = try ally.alloc(Index.RangeIterator, branch_count);
            for (0..branch_count) |i| {
                const bp = &rc.plan.logical_branches[i];
                self.bindIndex(bp.index);
                rc.or_iterators[i] = bp.index.range(bp.bounds.lower, bp.bounds.upper) catch
                    return error.ScanError;
            }
        }

        if (plan.eager) {
            try rc.prequery();
            rc.next = ListIterator.nextPrefetched;
        } else {
            if (plan.source == .index) {
                if (q.sector) |sector| {
                    rc.limitLeft = sector.limit;
                    if (sector.offset) |offset| rc.offsetLeft = offset;
                }
                rc.next = ListIterator.nextIndex;
            } else {
                if (q.sector) |sector| {
                    rc.limitLeft = sector.limit;
                    if (sector.offset) |offset| rc.offsetLeft = offset;
                }
                rc.next = ListIterator.nextUnfetched;
            }
        }

        if (q.cursor != null) {
            try rc.skipPastCursorAnchor();
        }

        return rc;
    }

    fn collectTargets(
        self: *Bucket,
        plan: *const QueryPlan,
        q: *const query.Query,
        targets: *std.ArrayList(DocumentLocation),
        arena: *std.heap.ArenaAllocator,
    ) error{ OutOfMemory, ScanError }!void {
        const ally = arena.allocator();

        switch (plan.source) {
            .full_scan => {
                var iterator = Bucket.ScanIterator.init(self, ally) catch |err| switch (err) {
                    error.OutOfMemory => return error.OutOfMemory,
                    else => return error.ScanError,
                };
                defer iterator.deinit();

                while (true) {
                    const maybe_doc = iterator.next() catch |err| switch (err) {
                        error.OutOfMemory => return error.OutOfMemory,
                        else => return error.ScanError,
                    } orelse break;

                    const doc = BSONDocument{ .buffer = maybe_doc.data };
                    if (q.filters.len == 0 or q.match(&doc)) {
                        try targets.append(ally, .{
                            .page_id = maybe_doc.page_id,
                            .offset = maybe_doc.offset,
                        });
                    }
                }
            },
            .index => {
                if (plan.index_strategy == .or_union) {
                    var seen = std.AutoHashMap(u128, void).init(ally);
                    defer seen.deinit();
                    for (0..plan.logical_branch_count) |i| {
                        const bp = &plan.logical_branches[i];
                        self.bindIndex(bp.index);
                        var iter = bp.index.range(bp.bounds.lower, bp.bounds.upper) catch return error.ScanError;
                        while (true) {
                            const maybe_loc = iter.next() catch |err| switch (err) {
                                error.OutOfMemory => return error.OutOfMemory,
                                else => return error.ScanError,
                            };
                            const loc = maybe_loc orelse break;
                            const key = docLocationKey(loc);
                            if (seen.contains(key)) continue;
                            try seen.put(key, {});
                            const doc = self.readDocAt(ally, .{
                                .page_id = loc.pageId,
                                .offset = loc.offset,
                            }) catch |err| switch (err) {
                                error.OutOfMemory => return error.OutOfMemory,
                                error.DocumentDeleted => continue,
                                else => return error.ScanError,
                            };
                            if (q.filters.len == 0 or q.match(&doc)) {
                                try targets.append(ally, .{
                                    .page_id = loc.pageId,
                                    .offset = loc.offset,
                                });
                            }
                        }
                    }
                    return;
                }

                if (plan.index_strategy == .nor_exclusion) {
                    var excluded = std.AutoHashMap(u128, void).init(ally);
                    defer excluded.deinit();
                    try self.collectExcludedDocIds(plan, &excluded);

                    var iterator = Bucket.ScanIterator.init(self, ally) catch |err| switch (err) {
                        error.OutOfMemory => return error.OutOfMemory,
                        else => return error.ScanError,
                    };
                    defer iterator.deinit();
                    while (true) {
                        const maybe_doc = iterator.next() catch |err| switch (err) {
                            error.OutOfMemory => return error.OutOfMemory,
                            else => return error.ScanError,
                        };
                        const doc_raw = maybe_doc orelse break;
                        if (excluded.contains(doc_raw.header.doc_id)) continue;
                        const doc = BSONDocument{ .buffer = doc_raw.data };

                        if (q.filters.len == 0 or q.match(&doc)) {
                            try targets.append(ally, .{
                                .page_id = doc_raw.page_id,
                                .offset = doc_raw.offset,
                            });
                        }
                    }
                    return;
                }

                const index_ptr = plan.index orelse return error.ScanError;
                self.bindIndex(index_ptr);

                if (Bucket.planUsesPointStrategy(plan)) {
                    const in_values = plan.point_values orelse return error.ScanError;

                    var seen = std.AutoHashMap(u128, void).init(ally);
                    defer seen.deinit();

                    var value_iter = in_values.array.iter();
                    while (value_iter.next()) |pair| {
                        if (!isIndexableValue(pair.value)) continue;

                        var range_iter = index_ptr.point(pair.value) catch return error.ScanError;
                        while (true) {
                            const maybe_loc = range_iter.next() catch |err| switch (err) {
                                error.OutOfMemory => return error.OutOfMemory,
                                else => return error.ScanError,
                            };
                            const loc = maybe_loc orelse break;
                            const key = docLocationKey(loc);
                            if (seen.contains(key)) continue;
                            try seen.put(key, {});

                            const doc = self.readDocAt(ally, .{
                                .page_id = loc.pageId,
                                .offset = loc.offset,
                            }) catch |err| switch (err) {
                                error.OutOfMemory => return error.OutOfMemory,
                                error.DocumentDeleted => continue,
                                else => return error.ScanError,
                            };

                            if (q.filters.len == 0 or q.match(&doc)) {
                                try targets.append(ally, .{
                                    .page_id = loc.pageId,
                                    .offset = loc.offset,
                                });
                            }
                        }
                    }
                } else {
                    var iterator = self.initIndexIterator(plan) catch return error.ScanError;
                    while (true) {
                        const maybe_loc = iterator.next() catch |err| switch (err) {
                            error.OutOfMemory => return error.OutOfMemory,
                            else => return error.ScanError,
                        };
                        const loc = maybe_loc orelse break;

                        const doc = self.readDocAt(ally, .{
                            .page_id = loc.pageId,
                            .offset = loc.offset,
                        }) catch |err| switch (err) {
                            error.OutOfMemory => return error.OutOfMemory,
                            error.DocumentDeleted => continue,
                            else => return error.ScanError,
                        };

                        if (q.filters.len == 0 or q.match(&doc)) {
                            try targets.append(ally, .{
                                .page_id = loc.pageId,
                                .offset = loc.offset,
                            });
                        }
                    }
                }
            },
        }
    }

    pub const TransformIterator = struct {
        bucket: *Bucket,
        tx: ?*Transaction = null,
        arena: *std.heap.ArenaAllocator,
        ally: std.mem.Allocator,
        query: query.Query,
        plan: QueryPlan,
        targets: []DocumentLocation,
        index: usize = 0,
        current_doc: ?BSONDocument = null,
        owns_arena: bool = false,

        pub const IteratorError = error{
            OutOfMemory,
            ScanError,
            IteratorDrained,
            DuplicateKey,
            InvalidTransform,
        };

        pub fn init(bucket: *Bucket, arena: *std.heap.ArenaAllocator, q: query.Query) !*TransformIterator {
            return bucket.transformIterate(arena, q);
        }

        fn ensureDoc(self: *TransformIterator) IteratorError!BSONDocument {
            if (self.current_doc) |doc| {
                return doc;
            }

            while (self.index < self.targets.len) {
                const target = self.targets[self.index];
                const doc = self.bucket.readDocAt(self.ally, target) catch |err| switch (err) {
                    error.OutOfMemory => return error.OutOfMemory,
                    else => return error.ScanError,
                };
                self.current_doc = doc;
                return doc;
            }

            return error.IteratorDrained;
        }

        pub fn data(self: *TransformIterator) IteratorError!?BSONDocument {
            if (self.index >= self.targets.len) {
                return null;
            }

            const doc = self.ensureDoc() catch |err| switch (err) {
                error.IteratorDrained => return null,
                else => return err,
            };

            return doc;
        }

        pub fn transform(self: *TransformIterator, updated: ?*const bson.BSONDocument) IteratorError!void {
            if (self.index >= self.targets.len) {
                return error.IteratorDrained;
            }

            var doc = try self.ensureDoc();

            var owned_strings = std.ArrayList([]u8){};
            defer {
                for (owned_strings.items) |str| {
                    self.bucket.allocator.free(str);
                }
                owned_strings.deinit(self.bucket.allocator);
            }

            const needs_id_clone = if (updated) |new_doc_ptr|
                new_doc_ptr.get("_id") == null
            else
                false;

            var cloned_id: ?BSONValue = null;
            if (needs_id_clone) {
                if (doc.get("_id")) |id_value| {
                    cloned_id = self.bucket.cloneIndexValue(id_value, &owned_strings) catch |err| switch (err) {
                        error.OutOfMemory => return error.OutOfMemory,
                    };
                }
            }

            var planned_index_deletes = std.ArrayList(PlannedIndexInsert){};
            defer {
                for (planned_index_deletes.items) |*plan| {
                    plan.values.deinit(self.bucket.allocator);
                }
                planned_index_deletes.deinit(self.bucket.allocator);
            }

            var idx_iter = self.bucket.indexes.iterator();
            while (idx_iter.next()) |pair| {
                const index_ptr = pair.value_ptr.*;
                const path = pair.key_ptr.*;

                var values = std.ArrayList(BSONValue){};
                var retain_values = false;
                defer {
                    if (!retain_values) values.deinit(self.bucket.allocator);
                }

                self.bucket.bindIndex(index_ptr);
                const has_values = self.bucket.gatherIndexValuesForPath(&doc, path, index_ptr.options, &values) catch |err| switch (err) {
                    error.OutOfMemory => return error.OutOfMemory,
                    error.IndexedStringTooLong => return error.ScanError,
                };
                if (!has_values) continue;

                try planned_index_deletes.append(self.bucket.allocator, .{
                    .index = index_ptr,
                    .values = values,
                });
                retain_values = true;
            }

            const target = self.targets[self.index];
            const autocommit = self.tx == null;

            var page = self.bucket.loadPageForWrite(target.page_id) catch |err| switch (err) {
                error.OutOfMemory => return error.OutOfMemory,
                else => return error.ScanError,
            };
            const header_offset = target.offset;
            if (header_offset + DocHeader.byteSize > page.data.len) {
                return error.ScanError;
            }

            const loc = Index.DocumentLocation{
                .pageId = target.page_id,
                .offset = target.offset,
            };

            var header = std.mem.bytesToValue(DocHeader, page.data[header_offset .. header_offset + DocHeader.byteSize]);
            if (header.is_deleted == 1) {
                // std.debug.print("Document at {any} is already deleted", .{loc});
                return error.IteratorDrained;
            }

            header.is_deleted = 1;
            const header_bytes = std.mem.toBytes(header);
            @memcpy(page.data[header_offset .. header_offset + header_bytes.len], &header_bytes);
            self.bucket.writePage(page) catch return error.OutOfMemory;

            for (planned_index_deletes.items) |plan| {
                self.bucket.bindIndex(plan.index);
                for (plan.values.items) |val| {
                    plan.index.delete(val, loc) catch |err| switch (err) {
                        error.OutOfMemory => return error.OutOfMemory,
                        else => return error.ScanError,
                    };
                }
            }

            self.bucket.header.doc_count -= 1;
            self.bucket.header.deleted_count += 1;
            self.bucket.flushHeader() catch return error.ScanError;

            if (updated) |new_doc_ptr| {
                var insert_doc = new_doc_ptr.*;
                var owns_doc = false;

                if (cloned_id) |id_val| {
                    if (insert_doc.get("_id") == null) {
                        insert_doc = try insert_doc.set(self.bucket.allocator, "_id", id_val);
                        owns_doc = true;
                    }
                }

                defer if (owns_doc) insert_doc.deinit(self.bucket.allocator);

                // Suppress oplog from inner insert; we emit an update oplog instead.
                self.bucket._suppress_oplog = true;
                defer {
                    self.bucket._suppress_oplog = false;
                }

                _ = self.bucket.insertLocked(insert_doc, autocommit) catch |err| switch (err) {
                    error.OutOfMemory => return error.OutOfMemory,
                    error.DuplicateKey => return error.DuplicateKey,
                    else => return error.ScanError,
                };

                // Emit a single update oplog entry with the new document.
                self.bucket.stageOplogEntry(.update, header.doc_id, insert_doc.buffer) catch return error.OutOfMemory;
            } else {
                // Pure delete (no replacement).
                if (autocommit) {
                    self.bucket.commitWalTransaction();
                }
                self.bucket.stageOplogEntry(.delete, header.doc_id, null) catch return error.OutOfMemory;
            }

            self.current_doc = null;
            _ = self.arena.reset(.retain_capacity);
            self.index += 1;
        }

        pub fn transfigurate(self: *TransformIterator, program: UpdateProgram) IteratorError!void {
            if (self.index >= self.targets.len) {
                return error.IteratorDrained;
            }

            const current = try self.ensureDoc();
            var updated = program.apply(self.bucket.allocator, current) catch |err| switch (err) {
                error.OutOfMemory => return error.OutOfMemory,
                else => return error.InvalidTransform,
            };
            defer updated.deinit(self.bucket.allocator);

            try self.transform(&updated);
        }

        pub fn transfigurateAll(self: *TransformIterator, program: UpdateProgram) IteratorError!usize {
            var updated: usize = 0;
            while (true) {
                const current = try self.data();
                if (current == null) break;
                try self.transfigurate(program);
                updated += 1;
            }
            return updated;
        }

        pub fn close(self: *TransformIterator) !void {
            const allocator = self.bucket.allocator;

            if (self.tx) |tx| {
                if (tx.open_transforms > 0) {
                    tx.open_transforms -= 1;
                }
            } else if (!self.bucket.in_memory and self.bucket.autoVaccuum and self.bucket.header.deleted_count > self.bucket.header.doc_count) {
                self.bucket.vacuum() catch |err| switch (err) {
                    error.OutOfMemory => return error.OutOfMemory,
                    else => return error.ScanError,
                };
            }
            // If we own the arena, clean it up
            if (self.owns_arena) {
                self.arena.deinit();
                allocator.destroy(self.arena);
            }
            allocator.free(self.targets);
            allocator.destroy(self);
        }
    };

    pub fn transformIterate(self: *Bucket, arena: *std.heap.ArenaAllocator, q: query.Query) !*TransformIterator {
        try self.ensureNoActiveTransaction();
        return self.transformIterateInternal(arena, q, null);
    }

    pub fn transfigurate(self: *Bucket, q: query.Query, program: UpdateProgram) !usize {
        var tx = try self.beginTransaction();
        var tx_closed = false;
        defer {
            if (!tx_closed) {
                tx.close() catch {};
            }
        }

        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();

        var iter = try tx.transformIterate(&arena, q);
        var iter_closed = false;
        defer {
            if (!iter_closed) {
                iter.close() catch {};
            }
        }

        const updated = try iter.transfigurateAll(program);
        try iter.close();
        iter_closed = true;

        try tx.commit();
        try tx.close();
        tx_closed = true;

        return updated;
    }

    fn transformIterateInternal(self: *Bucket, arena: *std.heap.ArenaAllocator, q: query.Query, tx: ?*Transaction) !*TransformIterator {
        var plan = self.planQuery(&q);

        var target_list = std.ArrayList(DocumentLocation){};
        defer target_list.deinit(arena.allocator());

        if (tx == null) {
            self.rwlock.lockShared();
            defer self.rwlock.unlockShared();
        }
        try self.collectTargets(&plan, &q, &target_list, arena);

        const raw_len = target_list.items.len;
        var targets_buf = try self.allocator.alloc(DocumentLocation, raw_len);
        errdefer self.allocator.free(targets_buf);
        mem.copyForwards(DocumentLocation, targets_buf[0..raw_len], target_list.items);

        var targets_slice = targets_buf[0..raw_len];
        if (q.sector) |sector| {
            const offset_value = sector.offset orelse 0;
            if (offset_value < targets_slice.len) {
                const start: usize = @intCast(offset_value);
                var end: usize = targets_slice.len;
                if (sector.limit) |limit_value| {
                    const limit_count: usize = @intCast(limit_value);
                    end = @min(start + limit_count, targets_slice.len);
                }
                if (start > 0) {
                    mem.copyForwards(
                        DocumentLocation,
                        targets_slice[0 .. end - start],
                        targets_slice[start..end],
                    );
                }
                targets_slice = targets_slice[0 .. end - start];
            } else {
                targets_slice = targets_slice[0..0];
            }
        }

        const iter = try self.allocator.create(TransformIterator);
        errdefer self.allocator.destroy(iter);
        iter.* = .{
            .bucket = self,
            .tx = tx,
            .arena = arena,
            .ally = arena.allocator(),
            .query = q,
            .plan = plan,
            .targets = targets_slice,
            .index = 0,
            .current_doc = null,
            .owns_arena = false,
        };

        if (tx) |active_tx| {
            active_tx.open_transforms += 1;
        }

        return iter;
    }

    pub fn list(self: *Bucket, allocator: std.mem.Allocator, q: query.Query) ![]BSONDocument {
        var plan = self.planQuery(&q);
        var docList: std.ArrayList(BSONDocument) = .{};
        if (q.sector) |sector| if (sector.limit) |limit| try docList.ensureTotalCapacity(allocator, limit);

        try self.collectDocs(&docList, allocator, &q, &plan);

        var resultSlice = docList.items;
        if (q.sortConfig) |sortConfig| {
            if (!plan.sort_covered) {
                std.mem.sort(BSONDocument, resultSlice, sortConfig, query.Query.sort);
            }
        }

        const offset = if (q.sector) |sector| sector.offset orelse 0 else 0;
        const limit = if (q.sector) |sector| sector.limit orelse resultSlice.len else resultSlice.len;

        return resultSlice[@min(resultSlice.len, offset)..@min(offset + limit, resultSlice.len)];
    }

    fn readDocAt(
        self: *Bucket,
        ally: mem.Allocator,
        loc: DocumentLocation,
    ) error{ OutOfMemory, PageNotFound, DocumentDeleted }!BSONDocument {
        var page = self.loadPage(loc.page_id) catch |err| switch (err) {
            error.OutOfMemory => return error.OutOfMemory,
            else => return error.PageNotFound,
        };
        var offset: u16 = loc.offset;

        if (offset + @sizeOf(DocHeader) > page.data.len) {
            return error.PageNotFound;
        }

        const header = std.mem.bytesToValue(DocHeader, page.data[offset .. offset + @sizeOf(DocHeader)]);
        if (header.is_deleted == 1) {
            return error.DocumentDeleted;
        }
        offset += @sizeOf(DocHeader);

        const doc_len_u32 = mem.readInt(u32, page.data[offset..][0..4], .little);
        const doc_len = @as(usize, doc_len_u32);

        var remaining = doc_len;
        const first_chunk = @min(doc_len, page.data.len - offset);
        // std.debug.print("WTF2 remaining {}, first_chunk {}\n", .{ remaining, first_chunk });
        if (first_chunk == doc_len) {
            return BSONDocument{
                .buffer = page.data[offset .. offset + doc_len],
            };
        }
        var docBuffer = try ally.alloc(u8, doc_len);
        @memcpy(docBuffer[0..first_chunk], page.data[offset .. offset + first_chunk]);
        remaining -= first_chunk;
        var written = first_chunk;

        var page_iterator = PageIterator{
            .bucket = self,
            .index = loc.page_id + 1,
            .type = .Data,
            .reverse = false,
        };

        while (remaining > 0) {
            const next_page = page_iterator.next() catch return error.PageNotFound;
            // std.debug.print("WTF remaining {}, page {}", .{ remaining, page_iterator.index });
            const page_ref = next_page orelse return error.PageNotFound;
            const chunk = @min(remaining, page_ref.data.len);
            @memcpy(docBuffer[written .. written + chunk], page_ref.data[0..chunk]);
            written += chunk;
            remaining -= chunk;
        }

        return BSONDocument{ .buffer = docBuffer };
    }

    fn readDocHeaderAt(
        self: *Bucket,
        loc: DocumentLocation,
    ) error{ OutOfMemory, PageNotFound, DocumentDeleted }!DocHeader {
        const page = self.loadPage(loc.page_id) catch |err| switch (err) {
            error.OutOfMemory => return error.OutOfMemory,
            else => return error.PageNotFound,
        };

        if (loc.offset + @sizeOf(DocHeader) > page.data.len) {
            return error.PageNotFound;
        }

        const header = std.mem.bytesToValue(DocHeader, page.data[loc.offset .. loc.offset + @sizeOf(DocHeader)]);
        if (header.is_deleted == 1) {
            return error.DocumentDeleted;
        }
        if (header.reserved != 0 or header.is_deleted > 1 or header.doc_id == 0) {
            return error.PageNotFound;
        }
        return header;
    }

    pub fn delete(self: *Bucket, q: query.Query) !void {
        try self.ensureNoActiveTransaction();
        self.rwlock.lock();
        defer self.rwlock.unlock();
        try self.deleteLocked(q, true);
    }

    fn deleteLocked(self: *Bucket, q: query.Query, autocommit: bool) !void {
        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const allocator = arena.allocator();
        var locations = std.ArrayList(DocumentMeta){};
        var iterator = try ScanIterator.init(self, allocator);

        // std.debug.print("iterator state: {any}\n", .{iterator.offset});
        while (try iterator.next()) |doc| {
            const deletable: BSONDocument = .{ .buffer = doc.data };
            const matched = q.match(&deletable);
            if (!matched) continue;
            try locations.append(allocator, .{
                .header = doc.header,
                .page_id = doc.page_id,
                .offset = doc.offset,
            });
        }

        for (locations.items) |*location| {
            // Read the full document first so we can compute indexed values
            // before tombstoning it.
            var doc = try self.readDocAt(allocator, .{ .page_id = location.page_id, .offset = location.offset });

            // Gather all per-index values to delete (mirrors insert path)
            var planned_index_deletes = std.ArrayList(PlannedIndexInsert){};
            defer {
                for (planned_index_deletes.items) |*plan| {
                    plan.values.deinit(self.allocator);
                }
                planned_index_deletes.deinit(self.allocator);
            }

            var idx_iter = self.indexes.iterator();
            while (idx_iter.next()) |pair| {
                const index_ptr = pair.value_ptr.*;
                const path = pair.key_ptr.*;

                var values = std.ArrayList(BSONValue){};
                var retain_values = false;
                defer {
                    if (!retain_values) values.deinit(self.allocator);
                }

                self.bindIndex(index_ptr);
                const has_values = try self.gatherIndexValuesForPath(&doc, path, index_ptr.options, &values);
                if (!has_values) continue; // sparse index missing field => nothing was inserted

                try planned_index_deletes.append(self.allocator, PlannedIndexInsert{
                    .index = index_ptr,
                    .values = values,
                });
                retain_values = true;
            }

            // Mark the document as deleted on its page (once per doc)
            var page = try self.loadPageForWrite(location.page_id);
            const header_offset = location.offset;
            if (header_offset + DocHeader.byteSize > page.data.len) {
                return error.PageNotFound;
            }
            var header = location.header;
            header.is_deleted = 1;
            const header_bytes = std.mem.toBytes(header);
            @memcpy(page.data[header_offset .. header_offset + header_bytes.len], &header_bytes);
            try self.writePage(page);

            // Remove all corresponding index entries for this doc/location
            const loc = Index.DocumentLocation{ .pageId = location.page_id, .offset = location.offset };
            for (planned_index_deletes.items) |plan| {
                self.bindIndex(plan.index);
                for (plan.values.items) |val| {
                    try plan.index.delete(val, loc);
                }
            }
        }

        self.header.doc_count -= locations.items.len;
        self.header.deleted_count += locations.items.len;
        try self.flushHeader();

        if (autocommit) {
            // Commit WAL transaction once per delete call.
            self.commitWalTransaction();
        }

        // Emit oplog entries for each deleted document.
        for (locations.items) |*location| {
            try self.stageOplogEntry(.delete, location.header.doc_id, null);
        }

        if (autocommit and !self.in_memory and self.header.deleted_count > self.header.doc_count and self.autoVaccuum) {
            // If all documents are deleted, reset the deleted count
            try self.vacuum();
        }
    }

    pub fn vacuum(self: *Bucket) !void {
        try self.ensureNoActiveTransaction();
        if (self.in_memory) {
            return;
        }

        const tempFileName = try std.fmt.allocPrint(self.allocator, "{s}-temp", .{self.path});
        defer self.allocator.free(tempFileName);
        const cache_capacity = self.pageCache.capacity;
        const reopen_wal = self.wal != null;
        const next_generation = try self.nextReplicationGeneration();
        var newBucket = try Bucket.openFileWithOptions(self.allocator, tempFileName, .{
            .page_cache_capacity = cache_capacity,
            .auto_vaccuum = self.autoVaccuum,
            .oplog_size = self.oplog_size,
        });
        defer newBucket.deinit();
        newBucket.header.replication_generation = next_generation;
        try newBucket.flushHeader();
        // defer fs.deleteFileAbsolute(tempFileName) catch |err| {
        // std.debug.print("Failed to delete existing temp file: {any}\n", .{err});
        // };
        var iterator = try ScanIterator.init(self, self.allocator);
        // const newMeta = try newBucket.loadPage(0);
        const oldMeta = try self.loadPage(0);
        // // @memcpy(newMeta.data, oldMeta.data);
        // // newBucket.loadIndices(newMeta) catch {
        // //     return BucketInitErrors.LoadIndexError;
        // // };
        // try newBucket.recordIndexes();
        var idxReader = std.io.Reader.fixed(oldMeta.data);
        while (true) {
            // Stop when we hit the first NUL at the beginning (no more entries)
            const b = try idxReader.peekByte();
            if (b == 0) break;

            // Read the index path (NUL-terminated); returned slice is inclusive of NUL
            const path_inclusive = idxReader.takeDelimiterInclusive(0) catch return PageError.InvalidPageSize;
            if (path_inclusive.len == 0) break; // defensive
            // Strip trailing NUL so map key does not include it
            const path_no_nul = path_inclusive[0 .. path_inclusive.len - 1];
            // const key = try self.allocator.dupe(u8, path_no_nul);
            // errdefer self.allocator.free(key);

            // Read index options
            const options = idxReader.takeStruct(IndexOptions, .little) catch return PageError.InvalidPageSize;

            // Read the index page ID
            _ = idxReader.takeInt(u64, .little) catch return PageError.InvalidPageSize;

            try newBucket.ensureIndex(path_no_nul, options);
        }

        while (try iterator.next()) |doc| {
            const newDoc = bson.BSONDocument.init(doc.data);
            _ = try newBucket.insert(newDoc);
        }

        // In WAL mode inserts went to the temp bucket's WAL.
        // Checkpoint now so the temp main file has all the data
        // before we swap it into place.
        try newBucket.flush();
        newBucket.checkpointWal();
        if (newBucket.wal) |*w| {
            w.consumeAndClose();
            newBucket.wal = null;
        }

        const path = try self.allocator.dupe(u8, self.path);
        iterator.deinit();
        self.deinit();

        platform.deleteFile(path) catch |err| {
            // std.debug.print("Failed to delete old file: {any}\n", .{err});
            return err;
        };
        platform.renameFile(tempFileName, path) catch |err| {
            // std.debug.print("Failed to rename temp file: {any}\n", .{err});
            return err;
        };

        self.file = try platform.openFile(self.allocator, path, .{
            .read = true,
            .write = true,
        });
        self.path = path;

        var header_bytes: [BucketHeader.byteSize]u8 = undefined;
        const file = if (self.file) |*f| f else return error.StorageUnavailable;
        try file.preadAll(header_bytes[0..], 0);
        self.header = BucketHeader.read(header_bytes[0..]);
        self.pageCache = PageCache.init(self.allocator, cache_capacity);
        self.cached_last_data_page = null;
        // Reinitialize and reload indexes from the meta page
        self.indexes = .init(self.allocator);
        self.wal_pending_pages = .init(self.allocator);
        self.wal_header_pending = false;
        self.oplog_size = newBucket.oplog_size;
        self.wal = if (reopen_wal) try initWal(self.allocator, path, self.oplog_size, self.header.replication_generation) else null;
        const meta = try self.loadPage(0);
        try self.loadIndices(meta);
    }

    pub fn deinit(self: *Bucket) void {
        if (self.active_tx) |tx| {
            tx.deinit();
        }

        // Flush any pending writes before cleanup
        self.flush() catch {
            // Ignore flush errors during cleanup
        };

        // Apply WAL frames to the main file and clean up.
        if (self.wal) |*w| {
            // Always checkpoint — even with active readers.  Other connections
            // will detect the bumped checkpoint_generation in SHM and invalidate
            // their page caches before the next read.
            if (self.nextReplicationGeneration()) |next_generation| {
                self.header.replication_generation = next_generation;
                self.persistHeaderToMainFile();
                w.setGeneration(next_generation) catch {};
                self.checkpointWal();
                w.consumeAndClose();
            } else |_| {
                // Preserve the WAL if generation space is exhausted so we never
                // silently wrap and reuse replication cursor history.
                w.deinit();
            }
            self.wal = null;
        }

        var idx_iter = self.indexes.iterator();
        while (idx_iter.next()) |pair| {
            const index_ptr = pair.value_ptr.*;
            const key = pair.key_ptr.*;
            self.allocator.destroy(index_ptr);
            self.allocator.free(key);
        }
        self.indexes.deinit();

        // Clear page cache (free all cached pages)
        self.pageCache.clear(self.allocator);
        self.pageCache.deinit();

        self.wal_pending_pages.deinit();

        // Free path
        self.allocator.free(self.path);

        // Close file last
        if (self.file) |*file| {
            file.close();
            self.file = null;
        }
    }
};

const testing = std.testing;

fn testListCount(bucket: *Bucket, allocator: std.mem.Allocator, json: []const u8) !usize {
    var query_doc = try bson.BSONDocument.fromJSON(allocator, json);
    defer query_doc.deinit(allocator);

    var q = try query.Query.parse(allocator, query_doc);
    defer q.deinit(allocator);

    var iter_arena = std.heap.ArenaAllocator.init(allocator);
    defer iter_arena.deinit();

    var iter = try bucket.listIterate(&iter_arena, q);
    defer iter.deinit() catch {};

    var count: usize = 0;
    while (try iter.next(iter)) |_| {
        count += 1;
    }
    return count;
}

test "Bucket.TransformIterator updates document" {
    var bucket_arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer bucket_arena.deinit();
    const allocator = bucket_arena.allocator();

    _ = platform.deleteFile("transform-update.bucket") catch {};
    var bucket = try Bucket.init(allocator, "transform-update.bucket");
    defer {
        bucket.deinit();
        platform.deleteFile("transform-update.bucket") catch {};
    }

    var insert_doc = try bson.fmt.serialize(.{ .name = "Alice", .age = 37 }, allocator);
    defer insert_doc.deinit(allocator);
    _ = try bucket.insert(insert_doc);

    var query_arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer query_arena.deinit();
    const q_alloc = query_arena.allocator();

    var query_doc = try bson.fmt.serialize(.{ .query = .{ .name = "Alice" } }, q_alloc);
    defer query_doc.deinit(q_alloc);

    var q = try query.Query.parse(q_alloc, query_doc);
    defer q.deinit(q_alloc);

    var iter_arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer iter_arena.deinit();

    var iter = try bucket.transformIterate(&iter_arena, q);
    defer iter.close() catch unreachable;

    const maybe_doc = try iter.data();
    try testing.expect(maybe_doc != null);
    const current = maybe_doc.?;

    const original_id = switch (current.get("_id").?) {
        .objectId => |obj| obj.value,
        else => unreachable,
    };

    var updated_doc = try bson.fmt.serialize(.{ .name = "Alice", .age = 42 }, q_alloc);
    defer updated_doc.deinit(q_alloc);

    try iter.transform(&updated_doc);

    const after = try iter.data();
    try testing.expect(after == null);

    var check_query_doc = try bson.fmt.serialize(
        .{ .query = .{ .name = "Alice" }, .sector = .{} },
        q_alloc,
    );
    defer check_query_doc.deinit(q_alloc);

    var check_query = try query.Query.parse(q_alloc, check_query_doc);
    defer check_query.deinit(q_alloc);

    var list_arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer list_arena.deinit();
    var list_iter = try bucket.listIterate(&list_arena, check_query);

    const updated_opt = try list_iter.next(list_iter);
    try testing.expect(updated_opt != null);
    const updated = updated_opt.?;

    const updated_id = switch (updated.get("_id").?) {
        .objectId => |obj| obj.value,
        else => unreachable,
    };
    try testing.expectEqualSlices(u8, original_id.buffer[0..], updated_id.buffer[0..]);

    const updated_age = updated.get("age").?.int32.value;
    try testing.expectEqual(@as(i32, 42), updated_age);

    const no_more = try list_iter.next(list_iter);
    try testing.expect(no_more == null);
}

test "Bucket.TransformIterator deletes document when null transform" {
    // var bucket_arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    // defer bucket_arena.deinit();
    const allocator = std.testing.allocator;

    _ = platform.deleteFile("transform-delete.bucket") catch {};
    var bucket = try Bucket.init(allocator, "transform-delete.bucket");
    defer {
        bucket.deinit();
        platform.deleteFile("transform-delete.bucket") catch {};
    }

    var insert_doc = try bson.fmt.serialize(.{ .name = "Bob", .age = 30 }, allocator);
    defer insert_doc.deinit(allocator);
    _ = try bucket.insert(insert_doc);

    var query_arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer query_arena.deinit();
    const q_alloc = query_arena.allocator();

    var query_doc = try bson.fmt.serialize(.{ .query = .{ .name = "Bob" } }, q_alloc);
    defer query_doc.deinit(q_alloc);

    var q = try query.Query.parse(q_alloc, query_doc);
    defer q.deinit(q_alloc);

    var iter_arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer iter_arena.deinit();

    var iter = try bucket.transformIterate(&iter_arena, q);
    defer iter.close() catch unreachable;

    try iter.transform(null);

    try testing.expectError(
        Bucket.TransformIterator.IteratorError.IteratorDrained,
        iter.transform(null),
    );

    const drained = try iter.data();
    try testing.expect(drained == null);

    var check_query_doc = try bson.fmt.serialize(.{ .query = .{ .name = "Bob" } }, q_alloc);
    defer check_query_doc.deinit(q_alloc);

    var check_query = try query.Query.parse(q_alloc, check_query_doc);
    defer check_query.deinit(q_alloc);

    var list_arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer list_arena.deinit();
    var list_iter = try bucket.listIterate(&list_arena, check_query);

    const remaining = try list_iter.next(list_iter);
    try testing.expect(remaining == null);
}

test "Bucket.transfigurate updates matching documents without manual iteration" {
    const allocator = std.testing.allocator;

    _ = platform.deleteFile("bucket-transfigurate.bucket") catch {};
    var bucket = try Bucket.init(allocator, "bucket-transfigurate.bucket");
    defer {
        bucket.deinit();
        platform.deleteFile("bucket-transfigurate.bucket") catch {};
    }

    const insert_doc = try bson.fmt.serialize(.{
        .name = "stark",
        .age = 40,
        .marriage = "pepper",
    }, allocator);
    defer insert_doc.deinit(allocator);
    _ = try bucket.insert(insert_doc);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const ally = arena.allocator();

    var query_doc = try bson.fmt.serialize(.{ .query = .{ .name = "stark" } }, ally);
    defer query_doc.deinit(ally);
    var q = try query.Query.parse(ally, query_doc);
    defer q.deinit(ally);

    var raw_program = try bson.fmt.serialize(.{
        .@"0" = .{
            .@"$set" = .{
                .age = .{ .@"$plus" = .{ "$.age", 1 } },
            },
            .state = "dead",
        },
        .@"1" = .{
            .@"$unset" = "marriage",
        },
    }, ally);
    defer raw_program.deinit(ally);
    var program = try UpdateProgram.parse(ally, raw_program);
    defer program.deinit(ally);

    try testing.expectEqual(@as(usize, 1), try bucket.transfigurate(q, program));

    var list_arena = std.heap.ArenaAllocator.init(allocator);
    defer list_arena.deinit();
    var list_iter = try bucket.listIterate(&list_arena, q);

    const updated = (try list_iter.next(list_iter)).?;
    try testing.expectEqual(@as(i32, 41), updated.get("age").?.int32.value);
    try testing.expectEqualStrings("dead", updated.get("state").?.string.value);
    try testing.expect(updated.get("marriage") == null);
    try testing.expect((try list_iter.next(list_iter)) == null);
}

test "Bucket.Transaction commit keeps writes hidden until commit" {
    const allocator = std.testing.allocator;
    const db_path = "transaction_commit_visibility.bucket";
    const wal_path = "transaction_commit_visibility.bucket-wal";
    const shm_path = "transaction_commit_visibility.bucket-wal-shm";

    platform.deleteFile(db_path) catch {};
    std.fs.cwd().deleteFile(wal_path) catch {};
    std.fs.cwd().deleteFile(shm_path) catch {};
    defer platform.deleteFile(db_path) catch {};
    defer std.fs.cwd().deleteFile(wal_path) catch {};
    defer std.fs.cwd().deleteFile(shm_path) catch {};

    var writer_arena = std.heap.ArenaAllocator.init(allocator);
    defer writer_arena.deinit();
    const w_ally = writer_arena.allocator();
    var writer = try Bucket.openFileWithOptions(w_ally, db_path, .{ .wal = true });
    defer writer.deinit();

    var reader_arena = std.heap.ArenaAllocator.init(allocator);
    defer reader_arena.deinit();
    const r_ally = reader_arena.allocator();
    var reader = try Bucket.openFileWithOptions(r_ally, db_path, .{ .wal = true });
    defer reader.deinit();

    const seed_doc = try bson.fmt.serialize(.{ .name = "seed" }, w_ally);
    _ = try writer.insert(seed_doc);

    var tx = try writer.beginTransaction();
    defer tx.close() catch unreachable;

    const staged_doc = try bson.fmt.serialize(.{ .name = "staged" }, w_ally);
    _ = try tx.insert(staged_doc);

    var delete_query_doc = try bson.fmt.serialize(.{ .query = .{ .name = "seed" } }, w_ally);
    defer delete_query_doc.deinit(w_ally);
    var delete_query = try query.Query.parse(w_ally, delete_query_doc);
    defer delete_query.deinit(w_ally);
    try tx.delete(delete_query);

    try testing.expectEqual(@as(usize, 1), try testListCount(&reader, r_ally, "{\"query\":{\"name\":\"seed\"}}"));
    try testing.expectEqual(@as(usize, 0), try testListCount(&reader, r_ally, "{\"query\":{\"name\":\"staged\"}}"));

    try tx.commit();

    try testing.expectEqual(@as(usize, 0), try testListCount(&reader, r_ally, "{\"query\":{\"name\":\"seed\"}}"));
    try testing.expectEqual(@as(usize, 1), try testListCount(&reader, r_ally, "{\"query\":{\"name\":\"staged\"}}"));
}

test "Bucket.Transaction rollback restores state" {
    const allocator = std.testing.allocator;
    const db_path = "transaction_rollback_restore.bucket";
    const wal_path = "transaction_rollback_restore.bucket-wal";
    const shm_path = "transaction_rollback_restore.bucket-wal-shm";

    platform.deleteFile(db_path) catch {};
    std.fs.cwd().deleteFile(wal_path) catch {};
    std.fs.cwd().deleteFile(shm_path) catch {};
    defer platform.deleteFile(db_path) catch {};
    defer std.fs.cwd().deleteFile(wal_path) catch {};
    defer std.fs.cwd().deleteFile(shm_path) catch {};

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const ally = arena.allocator();

    var bucket = try Bucket.openFileWithOptions(ally, db_path, .{ .wal = true });
    defer bucket.deinit();

    const original = try bson.fmt.serialize(.{ .name = "Alice" }, ally);
    _ = try bucket.insert(original);
    const snapshot_doc_count = bucket.header.doc_count;

    var tx = try bucket.beginTransaction();
    defer tx.close() catch unreachable;

    const staged = try bson.fmt.serialize(.{ .name = "Bob" }, ally);
    _ = try tx.insert(staged);

    var query_doc = try bson.fmt.serialize(.{ .query = .{ .name = "Alice" } }, ally);
    defer query_doc.deinit(ally);
    var q = try query.Query.parse(ally, query_doc);
    defer q.deinit(ally);

    var iter_arena = std.heap.ArenaAllocator.init(allocator);
    defer iter_arena.deinit();
    var iter = try tx.transformIterate(&iter_arena, q);
    try testing.expect((try iter.data()) != null);

    const updated = try bson.fmt.serialize(.{ .name = "Alicia" }, ally);
    try iter.transform(&updated);
    try iter.close();

    try tx.rollback();

    try testing.expectEqual(snapshot_doc_count, bucket.header.doc_count);
    try testing.expectEqual(@as(usize, 1), try testListCount(&bucket, ally, "{\"query\":{\"name\":\"Alice\"}}"));
    try testing.expectEqual(@as(usize, 0), try testListCount(&bucket, ally, "{\"query\":{\"name\":\"Alicia\"}}"));
    try testing.expectEqual(@as(usize, 0), try testListCount(&bucket, ally, "{\"query\":{\"name\":\"Bob\"}}"));
}

test "Bucket.Transaction transfigurate rolls back with the transaction" {
    const allocator = std.testing.allocator;
    const db_path = "transaction_transfigurate_rollback.bucket";
    const wal_path = "transaction_transfigurate_rollback.bucket-wal";
    const shm_path = "transaction_transfigurate_rollback.bucket-wal-shm";

    platform.deleteFile(db_path) catch {};
    std.fs.cwd().deleteFile(wal_path) catch {};
    std.fs.cwd().deleteFile(shm_path) catch {};
    defer platform.deleteFile(db_path) catch {};
    defer std.fs.cwd().deleteFile(wal_path) catch {};
    defer std.fs.cwd().deleteFile(shm_path) catch {};

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const ally = arena.allocator();

    var bucket = try Bucket.openFileWithOptions(ally, db_path, .{ .wal = true });
    defer bucket.deinit();

    const original = try bson.fmt.serialize(.{ .name = "Alice", .age = 30 }, ally);
    _ = try bucket.insert(original);

    var tx = try bucket.beginTransaction();
    defer tx.close() catch unreachable;

    var query_doc = try bson.fmt.serialize(.{ .query = .{ .name = "Alice" } }, ally);
    defer query_doc.deinit(ally);
    var q = try query.Query.parse(ally, query_doc);
    defer q.deinit(ally);

    var raw_program = try bson.fmt.serialize(.{
        .@"$set" = .{
            .age = .{ .@"$plus" = .{ "$.age", 2 } },
        },
    }, ally);
    defer raw_program.deinit(ally);
    var program = try UpdateProgram.parse(ally, raw_program);
    defer program.deinit(ally);

    try testing.expectEqual(@as(usize, 1), try tx.transfigurate(q, program));
    try tx.rollback();

    try testing.expectEqual(@as(usize, 1), try testListCount(&bucket, ally, "{\"query\":{\"name\":\"Alice\"}}"));

    var check_arena = std.heap.ArenaAllocator.init(allocator);
    defer check_arena.deinit();
    var list_iter = try bucket.listIterate(&check_arena, q);
    const doc = (try list_iter.next(list_iter)).?;
    try testing.expectEqual(@as(i32, 30), doc.get("age").?.int32.value);
}

test "Bucket.Transaction delays oplog until commit" {
    const allocator = std.testing.allocator;
    const db_path = "transaction_oplog_delay.bucket";
    const wal_path = "transaction_oplog_delay.bucket-wal";
    const shm_path = "transaction_oplog_delay.bucket-wal-shm";

    platform.deleteFile(db_path) catch {};
    std.fs.cwd().deleteFile(wal_path) catch {};
    std.fs.cwd().deleteFile(shm_path) catch {};
    defer platform.deleteFile(db_path) catch {};
    defer std.fs.cwd().deleteFile(wal_path) catch {};
    defer std.fs.cwd().deleteFile(shm_path) catch {};

    var writer_arena = std.heap.ArenaAllocator.init(allocator);
    defer writer_arena.deinit();
    const w_ally = writer_arena.allocator();
    var writer = try Bucket.openFileWithOptions(w_ally, db_path, .{ .wal = true });
    defer writer.deinit();

    var reader_arena = std.heap.ArenaAllocator.init(allocator);
    defer reader_arena.deinit();
    const r_ally = reader_arena.allocator();
    var reader = try Bucket.openFileWithOptions(r_ally, db_path, .{ .wal = true });
    defer reader.deinit();

    var qDoc = try bson.fmt.serialize(.{ .sector = .{} }, r_ally);
    defer qDoc.deinit(r_ally);
    const q = try query.Query.parse(r_ally, qDoc);
    var sub = try reader.subscribe(q);
    defer sub.deinit();

    var tx = try writer.beginTransaction();
    defer tx.close() catch unreachable;

    const staged = try bson.fmt.serialize(.{ .name = "delayed" }, w_ally);
    _ = try tx.insert(staged);

    try testing.expect((try sub.poll(64)) == null);

    try tx.commit();

    const batch = (try sub.poll(64)) orelse return error.TestExpectedEqual;
    const arr = batch.get("batch") orelse return error.TestExpectedEqual;
    try testing.expectEqual(@as(u32, 1), arr.array.keyNumber());
    const ev0 = arr.array.get("0") orelse return error.TestExpectedEqual;
    try testing.expectEqualStrings("insert", ev0.document.get("op").?.string.value);
}

test "Bucket.Transaction reduces WAL frame count" {
    const allocator = std.testing.allocator;
    const db_path_a = "transaction_wal_compare_a.bucket";
    const wal_path_a = "transaction_wal_compare_a.bucket-wal";
    const shm_path_a = "transaction_wal_compare_a.bucket-wal-shm";
    const db_path_b = "transaction_wal_compare_b.bucket";
    const wal_path_b = "transaction_wal_compare_b.bucket-wal";
    const shm_path_b = "transaction_wal_compare_b.bucket-wal-shm";

    platform.deleteFile(db_path_a) catch {};
    std.fs.cwd().deleteFile(wal_path_a) catch {};
    std.fs.cwd().deleteFile(shm_path_a) catch {};
    platform.deleteFile(db_path_b) catch {};
    std.fs.cwd().deleteFile(wal_path_b) catch {};
    std.fs.cwd().deleteFile(shm_path_b) catch {};
    defer platform.deleteFile(db_path_a) catch {};
    defer std.fs.cwd().deleteFile(wal_path_a) catch {};
    defer std.fs.cwd().deleteFile(shm_path_a) catch {};
    defer platform.deleteFile(db_path_b) catch {};
    defer std.fs.cwd().deleteFile(wal_path_b) catch {};
    defer std.fs.cwd().deleteFile(shm_path_b) catch {};

    var arena_a = std.heap.ArenaAllocator.init(allocator);
    defer arena_a.deinit();
    const a_ally = arena_a.allocator();
    var bucket_a = try Bucket.openFileWithOptions(a_ally, db_path_a, .{ .wal = true });
    defer bucket_a.deinit();

    const doc_a1 = try bson.fmt.serialize(.{ .seq = @as(i32, 1) }, a_ally);
    const doc_a2 = try bson.fmt.serialize(.{ .seq = @as(i32, 2) }, a_ally);
    _ = try bucket_a.insert(doc_a1);
    _ = try bucket_a.insert(doc_a2);
    const standalone_frames = bucket_a.wal.?.live_frame_count;

    var arena_b = std.heap.ArenaAllocator.init(allocator);
    defer arena_b.deinit();
    const b_ally = arena_b.allocator();
    var bucket_b = try Bucket.openFileWithOptions(b_ally, db_path_b, .{ .wal = true });
    defer bucket_b.deinit();

    var tx = try bucket_b.beginTransaction();
    defer tx.close() catch unreachable;
    const doc_b1 = try bson.fmt.serialize(.{ .seq = @as(i32, 1) }, b_ally);
    const doc_b2 = try bson.fmt.serialize(.{ .seq = @as(i32, 2) }, b_ally);
    _ = try tx.insert(doc_b1);
    _ = try tx.insert(doc_b2);
    try tx.commit();
    const transaction_frames = bucket_b.wal.?.live_frame_count;

    try testing.expect(transaction_frames < standalone_frames);
}

test "Bucket.Transaction guardrails" {
    const allocator = std.testing.allocator;
    const db_path = "transaction_guardrails.bucket";
    const wal_path = "transaction_guardrails.bucket-wal";
    const shm_path = "transaction_guardrails.bucket-wal-shm";

    platform.deleteFile(db_path) catch {};
    std.fs.cwd().deleteFile(wal_path) catch {};
    std.fs.cwd().deleteFile(shm_path) catch {};
    defer platform.deleteFile(db_path) catch {};
    defer std.fs.cwd().deleteFile(wal_path) catch {};
    defer std.fs.cwd().deleteFile(shm_path) catch {};

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const ally = arena.allocator();
    var bucket = try Bucket.openFileWithOptions(ally, db_path, .{ .wal = true });
    defer bucket.deinit();

    const base = try bson.fmt.serialize(.{ .name = "guard" }, ally);
    _ = try bucket.insert(base);

    var tx = try bucket.beginTransaction();
    defer tx.close() catch unreachable;

    try testing.expectError(error.TransactionActive, bucket.beginTransaction());
    const blocked = try bson.fmt.serialize(.{ .name = "blocked" }, ally);
    try testing.expectError(error.TransactionActive, bucket.insert(blocked));

    var query_doc = try bson.fmt.serialize(.{ .query = .{ .name = "guard" } }, ally);
    defer query_doc.deinit(ally);
    var q = try query.Query.parse(ally, query_doc);
    defer q.deinit(ally);

    var iter_arena = std.heap.ArenaAllocator.init(allocator);
    defer iter_arena.deinit();
    var iter = try tx.transformIterate(&iter_arena, q);
    defer iter.close() catch unreachable;

    try testing.expectError(error.TransactionBusy, tx.commit());
    try testing.expectError(error.TransactionBusy, tx.rollback());
}

test "Bucket.insert" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();
    var bucket = try Bucket.init(allocator, "test.bucket");
    defer bucket.deinit();
    defer platform.deleteFile("test.bucket") catch |err| {
        std.debug.print("Failed to delete test file: {any}\n", .{err});
    };

    // Create a new BSON document
    var doc = try bson.fmt.serialize(.{ .name = "Alice", .age = 37 }, allocator);
    defer doc.deinit(allocator);

    // Insert the document into the bucket
    _ = try bucket.insert(doc);

    const qDoc = try bson.fmt.serialize(.{ .sector = .{} }, allocator);
    const q1 = try query.Query.parse(allocator, qDoc);

    var res1list = std.ArrayList(BSONDocument){};
    defer res1list.deinit(allocator);
    var res1iter = try bucket.listIterate(&arena, q1);
    while (try res1iter.next(res1iter)) |docItem| {
        try res1list.append(allocator, docItem);
    }
    const res1 = res1list.items;

    try testing.expect(res1.len == 1);
    try testing.expectEqualStrings(res1[0].get("name").?.string.value, "Alice");

    var q2Doc = try bson.fmt.serialize(.{ .query = .{ .name = "Alice" } }, allocator);
    defer q2Doc.deinit(allocator);
    const q2 = try query.Query.parse(allocator, q2Doc);

    var res2list = std.ArrayList(BSONDocument){};
    defer res2list.deinit(allocator);
    var res2iter = try bucket.listIterate(&arena, q2);
    while (try res2iter.next(res2iter)) |docItem| {
        try res2list.append(allocator, docItem);
    }
    const res2 = res2list.items;

    try testing.expectEqual(res1.len, res2.len);
    try testing.expectEqualStrings(res1[0].get("name").?.string.value, "Alice");

    // for (qResult) |item| {
    //     // const oId = item.get("_id").?.objectId.value;
    // std.debug.print("Document _id: {s}, timestamp: {any}\n", .{ oId.toString(), oId });
    // }
}

test "Bucket.listIterate eager sort applies offset once" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const file_name = "sort-offset-once.bucket";
    defer platform.deleteFile(file_name) catch {};

    var bucket = try Bucket.init(allocator, file_name);
    defer bucket.deinit();

    // Insert out of order so sorting is required.
    var doc_c = try bson.fmt.serialize(.{ .name = "C" }, allocator);
    defer doc_c.deinit(allocator);
    _ = try bucket.insert(doc_c);

    var doc_a = try bson.fmt.serialize(.{ .name = "A" }, allocator);
    defer doc_a.deinit(allocator);
    _ = try bucket.insert(doc_a);

    var doc_b = try bson.fmt.serialize(.{ .name = "B" }, allocator);
    defer doc_b.deinit(allocator);
    _ = try bucket.insert(doc_b);

    var q_doc = try bson.fmt.serialize(.{
        .sort = .{ .asc = "name" },
        .sector = .{ .offset = 1, .limit = 1 },
    }, allocator);
    defer q_doc.deinit(allocator);

    var q = try query.Query.parse(allocator, q_doc);
    defer q.deinit(allocator);

    var iter_arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer iter_arena.deinit();

    var iter = try bucket.listIterate(&iter_arena, q);
    defer iter.deinit() catch {};

    const first = (try iter.next(iter)).?;
    try testing.expectEqualStrings("B", first.get("name").?.string.value);
    try testing.expectEqual(@as(?BSONDocument, null), try iter.next(iter));
}

test "Bucket.unique index rejects duplicate string values" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var bucket = try Bucket.init(allocator, ":memory:");
    defer bucket.deinit();

    try bucket.ensureIndex("email", .{ .unique = 1 });

    var doc1 = try bson.fmt.serialize(.{ .email = "a@example.com" }, allocator);
    defer doc1.deinit(allocator);
    _ = try bucket.insert(doc1);

    var doc2 = try bson.fmt.serialize(.{ .email = "a@example.com" }, allocator);
    defer doc2.deinit(allocator);
    try std.testing.expectError(error.DuplicateKey, bucket.insert(doc2));

    // Ensure no partial write occurred
    var qDoc = try bson.fmt.serialize(.{ .sector = .{} }, allocator);
    defer qDoc.deinit(allocator);
    var q = try query.Query.parse(allocator, qDoc);
    defer q.deinit(allocator);

    var list_arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer list_arena.deinit();
    var iter = try bucket.listIterate(&list_arena, q);

    var count: usize = 0;
    while (try iter.next(iter)) |_| {
        count += 1;
    }
    try std.testing.expectEqual(@as(usize, 1), count);
}

test "Bucket._id unique index rejects duplicate ObjectId" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var bucket = try Bucket.init(allocator, ":memory:");
    defer bucket.deinit();

    const oid = try ObjectId.parseString("507c7f79bcf86cd7994f6c0e");
    const oid2 = try ObjectId.parseString("507c7f79bcf86cd7994f6c0f");

    var doc1 = try bson.fmt.serialize(.{ ._id = oid, .name = "Alice" }, allocator);
    defer doc1.deinit(allocator);
    _ = try bucket.insert(doc1);

    var doc2 = try bson.fmt.serialize(.{ ._id = oid, .name = "Bob" }, allocator);
    defer doc2.deinit(allocator);
    try std.testing.expectError(error.DuplicateKey, bucket.insert(doc2));

    // Different ObjectId should still insert fine
    var doc3 = try bson.fmt.serialize(.{ ._id = oid2, .name = "Carol" }, allocator);
    defer doc3.deinit(allocator);
    _ = try bucket.insert(doc3);
}

test "Bucket.dump supports :memory: storage" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var in_mem_bucket = try Bucket.init(allocator, ":memory:");
    defer in_mem_bucket.deinit();

    var doc = try bson.fmt.serialize(.{ .name = "InMemory" }, allocator);
    defer doc.deinit(allocator);
    _ = try in_mem_bucket.insert(doc);

    const dump_path = "memory_dump.bucket";
    defer platform.deleteFile(dump_path) catch |err| {
        std.debug.print("Failed to delete dump file: {any}\n", .{err});
    };

    try in_mem_bucket.dump(dump_path);

    var disk_bucket = try Bucket.init(allocator, dump_path);
    defer disk_bucket.deinit();

    var qDoc = try bson.fmt.serialize(.{ .sector = .{} }, allocator);
    defer qDoc.deinit(allocator);
    const q = try query.Query.parse(allocator, qDoc);

    var iter = try disk_bucket.listIterate(&arena, q);
    defer {
        iter.deinit() catch {};
    }

    const maybe_doc = try iter.next(iter);
    try testing.expect(maybe_doc != null);
    const stored = maybe_doc.?;
    defer allocator.free(stored.buffer);

    try testing.expectEqualStrings(stored.get("name").?.string.value, "InMemory");
}

fn setupIndexQueryBucket(bucket: *Bucket, allocator: std.mem.Allocator) !void {
    try bucket.ensureIndex("age", .{});
    try bucket.ensureIndex("scores", .{ .sparse = 1 });
    try bucket.ensureIndex("name", .{});

    var alice_doc = try bson.fmt.serialize(.{ .name = "Alice", .age = 30, .scores = [_]i32{ 5, 7 } }, allocator);
    defer alice_doc.deinit(allocator);
    _ = try bucket.insert(alice_doc);

    var bob_doc = try bson.fmt.serialize(.{ .name = "Bob", .age = 40, .scores = [_]i32{9} }, allocator);
    defer bob_doc.deinit(allocator);
    _ = try bucket.insert(bob_doc);

    var carol_doc = try bson.fmt.serialize(.{ .name = "Carol", .age = 50 }, allocator);
    defer carol_doc.deinit(allocator);
    _ = try bucket.insert(carol_doc);

    var dora_doc = try bson.fmt.serialize(.{ .name = "Dora", .age = 35, .scores = [_]i32{7} }, allocator);
    defer dora_doc.deinit(allocator);
    _ = try bucket.insert(dora_doc);
}

fn queryDocWithCursor(allocator: std.mem.Allocator, base: BSONDocument, cursor: BSONDocument) !BSONDocument {
    var editor = bson.Editor.init(allocator, base);
    defer editor.deinit();
    try editor.setValue("cursor", BSONValue.init(cursor));
    return try editor.finish();
}

test "Bucket.full scan cursor resumes after partial consumption" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = std.testing.allocator;
    platform.deleteFile("cursor-full-scan.bucket") catch {};
    var bucket = try Bucket.init(allocator, "cursor-full-scan.bucket");
    defer bucket.deinit();
    defer platform.deleteFile("cursor-full-scan.bucket") catch {};

    const docs = [_][]const u8{ "A", "B", "C" };
    for (docs) |name| {
        var doc = try bson.fmt.serialize(.{ .name = name }, allocator);
        defer doc.deinit(allocator);
        _ = try bucket.insert(doc);
    }

    var qdoc = try bson.fmt.serialize(.{}, allocator);
    defer qdoc.deinit(allocator);
    var q = try query.Query.parse(allocator, qdoc);
    defer q.deinit(allocator);

    var iter = try bucket.listIterate(&arena, q);
    defer iter.deinit() catch {};

    const first = (try iter.next(iter)).?;
    const second = (try iter.next(iter)).?;
    try testing.expectEqualStrings("A", first.get("name").?.string.value);
    try testing.expectEqualStrings("B", second.get("name").?.string.value);

    var cursor_doc = try iter.exportCursor(allocator);
    defer cursor_doc.deinit(allocator);

    var resume_doc = try queryDocWithCursor(allocator, qdoc, cursor_doc);
    defer resume_doc.deinit(allocator);
    var resume_query = try query.Query.parse(allocator, resume_doc);
    defer resume_query.deinit(allocator);

    var resume_arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer resume_arena.deinit();
    var resumed = try bucket.listIterate(&resume_arena, resume_query);
    defer resumed.deinit() catch {};

    const final = (try resumed.next(resumed)).?;
    try testing.expectEqualStrings("C", final.get("name").?.string.value);
    try testing.expectEqual(@as(?BSONDocument, null), try resumed.next(resumed));
}

test "Bucket.index range cursor resumes after partial consumption" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = std.testing.allocator;
    platform.deleteFile("cursor-index-range.bucket") catch {};
    var bucket = try Bucket.init(allocator, "cursor-index-range.bucket");
    try setupIndexQueryBucket(&bucket, allocator);
    defer bucket.deinit();
    defer platform.deleteFile("cursor-index-range.bucket") catch {};

    var qdoc = try bson.fmt.serialize(.{ .query = .{ .age = .{ .@"$gte" = 35 } } }, allocator);
    defer qdoc.deinit(allocator);
    var q = try query.Query.parse(allocator, qdoc);
    defer q.deinit(allocator);

    var iter = try bucket.listIterate(&arena, q);
    defer iter.deinit() catch {};

    const first = (try iter.next(iter)).?;
    try testing.expectEqualStrings("Dora", first.get("name").?.string.value);

    var cursor_doc = try iter.exportCursor(allocator);
    defer cursor_doc.deinit(allocator);

    var resume_doc = try queryDocWithCursor(allocator, qdoc, cursor_doc);
    defer resume_doc.deinit(allocator);
    var resume_query = try query.Query.parse(allocator, resume_doc);
    defer resume_query.deinit(allocator);

    var resume_arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer resume_arena.deinit();
    var resumed = try bucket.listIterate(&resume_arena, resume_query);
    defer resumed.deinit() catch {};

    const next = (try resumed.next(resumed)).?;
    const last = (try resumed.next(resumed)).?;
    try testing.expectEqualStrings("Bob", next.get("name").?.string.value);
    try testing.expectEqualStrings("Carol", last.get("name").?.string.value);
    try testing.expectEqual(@as(?BSONDocument, null), try resumed.next(resumed));
}

test "Bucket.cursor exported before first read resumes from beginning" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = std.testing.allocator;
    platform.deleteFile("cursor-before-first-read.bucket") catch {};
    var bucket = try Bucket.init(allocator, "cursor-before-first-read.bucket");
    defer bucket.deinit();
    defer platform.deleteFile("cursor-before-first-read.bucket") catch {};

    var doc = try bson.fmt.serialize(.{ .name = "Alpha" }, allocator);
    defer doc.deinit(allocator);
    _ = try bucket.insert(doc);

    var qdoc = try bson.fmt.serialize(.{}, allocator);
    defer qdoc.deinit(allocator);
    var q = try query.Query.parse(allocator, qdoc);
    defer q.deinit(allocator);

    var iter = try bucket.listIterate(&arena, q);
    defer iter.deinit() catch {};
    var cursor_doc = try iter.exportCursor(allocator);
    defer cursor_doc.deinit(allocator);

    var resume_doc = try queryDocWithCursor(allocator, qdoc, cursor_doc);
    defer resume_doc.deinit(allocator);
    var resume_query = try query.Query.parse(allocator, resume_doc);
    defer resume_query.deinit(allocator);

    var resume_arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer resume_arena.deinit();
    var resumed = try bucket.listIterate(&resume_arena, resume_query);
    defer resumed.deinit() catch {};

    const first = (try resumed.next(resumed)).?;
    try testing.expectEqualStrings("Alpha", first.get("name").?.string.value);
}

test "Bucket.cursor exported after eos resumes from end and sees later inserts" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = std.testing.allocator;
    platform.deleteFile("cursor-after-eos.bucket") catch {};
    var bucket = try Bucket.init(allocator, "cursor-after-eos.bucket");
    defer bucket.deinit();
    defer platform.deleteFile("cursor-after-eos.bucket") catch {};

    var doc1 = try bson.fmt.serialize(.{ .name = "A" }, allocator);
    defer doc1.deinit(allocator);
    _ = try bucket.insert(doc1);
    var doc2 = try bson.fmt.serialize(.{ .name = "B" }, allocator);
    defer doc2.deinit(allocator);
    _ = try bucket.insert(doc2);

    var qdoc = try bson.fmt.serialize(.{}, allocator);
    defer qdoc.deinit(allocator);
    var q = try query.Query.parse(allocator, qdoc);
    defer q.deinit(allocator);

    var iter = try bucket.listIterate(&arena, q);
    defer iter.deinit() catch {};
    while (try iter.next(iter)) |_| {}

    var cursor_doc = try iter.exportCursor(allocator);
    defer cursor_doc.deinit(allocator);

    var doc3 = try bson.fmt.serialize(.{ .name = "C" }, allocator);
    defer doc3.deinit(allocator);
    _ = try bucket.insert(doc3);

    var resume_doc = try queryDocWithCursor(allocator, qdoc, cursor_doc);
    defer resume_doc.deinit(allocator);
    var resume_query = try query.Query.parse(allocator, resume_doc);
    defer resume_query.deinit(allocator);

    var resume_arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer resume_arena.deinit();
    var resumed = try bucket.listIterate(&resume_arena, resume_query);
    defer resumed.deinit() catch {};

    const next = (try resumed.next(resumed)).?;
    try testing.expectEqualStrings("C", next.get("name").?.string.value);
    try testing.expectEqual(@as(?BSONDocument, null), try resumed.next(resumed));
}

test "Bucket.cursor with deleted anchor returns invalid cursor" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = std.testing.allocator;
    platform.deleteFile("cursor-deleted-anchor.bucket") catch {};
    var bucket = try Bucket.init(allocator, "cursor-deleted-anchor.bucket");
    defer bucket.deinit();
    defer platform.deleteFile("cursor-deleted-anchor.bucket") catch {};

    var doc1 = try bson.fmt.serialize(.{ .name = "A" }, allocator);
    defer doc1.deinit(allocator);
    _ = try bucket.insert(doc1);
    var doc2 = try bson.fmt.serialize(.{ .name = "B" }, allocator);
    defer doc2.deinit(allocator);
    _ = try bucket.insert(doc2);

    var qdoc = try bson.fmt.serialize(.{}, allocator);
    defer qdoc.deinit(allocator);
    var q = try query.Query.parse(allocator, qdoc);
    defer q.deinit(allocator);

    var iter = try bucket.listIterate(&arena, q);
    defer iter.deinit() catch {};
    _ = (try iter.next(iter)).?;
    var cursor_doc = try iter.exportCursor(allocator);
    defer cursor_doc.deinit(allocator);

    var delete_qdoc = try bson.fmt.serialize(.{ .query = .{ .name = "A" } }, allocator);
    defer delete_qdoc.deinit(allocator);
    var delete_query = try query.Query.parse(allocator, delete_qdoc);
    defer delete_query.deinit(allocator);
    try bucket.delete(delete_query);

    var resume_doc = try queryDocWithCursor(allocator, qdoc, cursor_doc);
    defer resume_doc.deinit(allocator);
    var resume_query = try query.Query.parse(allocator, resume_doc);
    defer resume_query.deinit(allocator);

    var resume_arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer resume_arena.deinit();
    try testing.expectError(error.InvalidCursor, bucket.listIterate(&resume_arena, resume_query));
}

test "Bucket.cursor query rejects sort" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = std.testing.allocator;
    platform.deleteFile("cursor-sort-unsupported.bucket") catch {};
    var bucket = try Bucket.init(allocator, "cursor-sort-unsupported.bucket");
    defer bucket.deinit();
    defer platform.deleteFile("cursor-sort-unsupported.bucket") catch {};

    var qdoc = try bson.fmt.serialize(.{
        .sort = .{ .asc = "name" },
        .cursor = .{ .version = 1, .mode = "full_scan"[0..] },
    }, allocator);
    defer qdoc.deinit(allocator);
    var q = try query.Query.parse(allocator, qdoc);
    defer q.deinit(allocator);

    try testing.expectError(error.UnsupportedCursorQuery, bucket.listIterate(&arena, q));
}

test "Bucket.cursor query rejects sector" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = std.testing.allocator;
    platform.deleteFile("cursor-sector-unsupported.bucket") catch {};
    var bucket = try Bucket.init(allocator, "cursor-sector-unsupported.bucket");
    defer bucket.deinit();
    defer platform.deleteFile("cursor-sector-unsupported.bucket") catch {};

    var qdoc = try bson.fmt.serialize(.{
        .sector = .{ .limit = 1 },
        .cursor = .{ .version = 1, .mode = "full_scan"[0..] },
    }, allocator);
    defer qdoc.deinit(allocator);
    var q = try query.Query.parse(allocator, qdoc);
    defer q.deinit(allocator);

    try testing.expectError(error.UnsupportedCursorQuery, bucket.listIterate(&arena, q));
}

test "Bucket.cursor query rejects point strategy index scans" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = std.testing.allocator;
    platform.deleteFile("cursor-point-unsupported.bucket") catch {};
    var bucket = try Bucket.init(allocator, "cursor-point-unsupported.bucket");
    try setupIndexQueryBucket(&bucket, allocator);
    defer bucket.deinit();
    defer platform.deleteFile("cursor-point-unsupported.bucket") catch {};

    var qdoc = try bson.fmt.serialize(.{
        .query = .{ .age = .{ .@"$in" = [_]i32{ 30, 40 } } },
        .cursor = .{ .version = 1, .mode = "index_range"[0..], .indexPath = "age"[0..] },
    }, allocator);
    defer qdoc.deinit(allocator);
    var q = try query.Query.parse(allocator, qdoc);
    defer q.deinit(allocator);

    try testing.expectError(error.UnsupportedCursorQuery, bucket.listIterate(&arena, q));
}

test "Bucket.indexed query equality uses range" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = std.testing.allocator;
    platform.deleteFile("index-query-eq.bucket") catch {};
    var bucket = try Bucket.init(allocator, "index-query-eq.bucket");
    try setupIndexQueryBucket(&bucket, allocator);
    defer bucket.deinit();
    defer platform.deleteFile("index-query-eq.bucket") catch {};

    var qdoc = try bson.fmt.serialize(.{ .query = .{ .age = 30 } }, allocator);
    defer qdoc.deinit(allocator);

    var q = try query.Query.parse(allocator, qdoc);
    defer q.deinit(allocator);

    const plan = bucket.planQuery(&q);
    try testing.expect(plan.source == .index);
    try testing.expect(plan.index_strategy == Bucket.QueryPlan.IndexStrategy.range);
    try testing.expect(plan.index_path != null and std.mem.eql(u8, plan.index_path.?, "age"));

    var iter = try bucket.listIterate(&arena, q);
    defer iter.deinit() catch {};
    var count: usize = 0;
    while (try iter.next(iter)) |doc| {
        const name = doc.get("name").?.string.value;
        try testing.expect(std.mem.eql(u8, name, "Alice"));
        count += 1;
    }
    try testing.expectEqual(@as(usize, 1), count);
}

test "Bucket.indexed query startsWith uses range" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = std.testing.allocator;
    platform.deleteFile("index-query-startswith.bucket") catch {};
    var bucket = try Bucket.init(allocator, "index-query-startswith.bucket");
    try setupIndexQueryBucket(&bucket, allocator);
    defer bucket.deinit();
    defer platform.deleteFile("index-query-startswith.bucket") catch {};

    var qdoc = try bson.fmt.serialize(.{ .query = .{ .name = .{ .@"$startsWith" = "A" } } }, allocator);
    defer qdoc.deinit(allocator);

    var q = try query.Query.parse(allocator, qdoc);
    defer q.deinit(allocator);

    const plan = bucket.planQuery(&q);
    try testing.expect(plan.source == .index);
    try testing.expect(plan.index_strategy == Bucket.QueryPlan.IndexStrategy.range);
    try testing.expect(plan.index_path != null and std.mem.eql(u8, plan.index_path.?, "name"));
    try testing.expect(plan.bounds.lower != null);
    try testing.expect(plan.bounds.upper == null);

    var iter = try bucket.listIterate(&arena, q);
    defer iter.deinit() catch {};
    var count: usize = 0;
    while (try iter.next(iter)) |doc| {
        const name = doc.get("name").?.string.value;
        try testing.expect(std.mem.eql(u8, name, "Alice"));
        count += 1;
    }
    try testing.expectEqual(@as(usize, 1), count);
}

test "Bucket.indexed query range bounds combine" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = std.testing.allocator;
    platform.deleteFile("index-query-range.bucket") catch {};
    var bucket = try Bucket.init(allocator, "index-query-range.bucket");
    try setupIndexQueryBucket(&bucket, allocator);
    defer bucket.deinit();
    defer platform.deleteFile("index-query-range.bucket") catch {};

    var qdoc = try bson.fmt.serialize(
        .{ .query = .{ .age = .{ .@"$gte" = 35, .@"$lte" = 40 } } },
        allocator,
    );
    defer qdoc.deinit(allocator);

    var q = try query.Query.parse(allocator, qdoc);
    defer q.deinit(allocator);

    const plan = bucket.planQuery(&q);
    try testing.expect(plan.source == .index);
    try testing.expect(plan.index_strategy == Bucket.QueryPlan.IndexStrategy.range);
    try testing.expect(plan.index_path != null and std.mem.eql(u8, plan.index_path.?, "age"));
    try testing.expect(plan.bounds.lower != null);
    try testing.expect(plan.bounds.upper != null);
    try testing.expect(plan.bounds.lower.?.filter == .gte);
    try testing.expect(plan.bounds.upper.?.filter == .lte);
    switch (plan.bounds.lower.?.value) {
        .int32 => |v| try testing.expect(v.value == 35),
        else => try testing.expect(false),
    }
    switch (plan.bounds.upper.?.value) {
        .int32 => |v| try testing.expect(v.value == 40),
        else => try testing.expect(false),
    }

    var iter = try bucket.listIterate(&arena, q);
    defer iter.deinit() catch {};
    var seen_dora = false;
    var seen_bob = false;
    var count: usize = 0;
    while (try iter.next(iter)) |doc| {
        const name = doc.get("name").?.string.value;
        if (std.mem.eql(u8, name, "Dora")) {
            try testing.expect(!seen_dora);
            seen_dora = true;
        } else if (std.mem.eql(u8, name, "Bob")) {
            try testing.expect(!seen_bob);
            seen_bob = true;
        } else {
            try testing.expect(false);
        }
        count += 1;
    }
    try testing.expectEqual(@as(usize, 2), count);
    try testing.expect(seen_dora);
    try testing.expect(seen_bob);
}

test "Bucket.indexed query in uses points" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = std.testing.allocator;
    platform.deleteFile("index-query-in.bucket") catch {};
    var bucket = try Bucket.init(allocator, "index-query-in.bucket");
    try setupIndexQueryBucket(&bucket, allocator);
    defer bucket.deinit();
    defer platform.deleteFile("index-query-in.bucket") catch {};

    var qdoc = try bson.fmt.serialize(.{ .query = .{ .age = .{ .@"$in" = [_]i32{ 30, 40 } } } }, allocator);
    defer qdoc.deinit(allocator);

    var q = try query.Query.parse(allocator, qdoc);
    defer q.deinit(allocator);

    const plan = bucket.planQuery(&q);
    try testing.expect(plan.source == .index);
    try testing.expect(plan.index_strategy == Bucket.QueryPlan.IndexStrategy.points);
    try testing.expect(plan.index_path != null and std.mem.eql(u8, plan.index_path.?, "age"));

    var iter = try bucket.listIterate(&arena, q);
    defer iter.deinit() catch {};
    var seen_alice = false;
    var seen_bob = false;
    var count: usize = 0;
    while (try iter.next(iter)) |doc| {
        const name = doc.get("name").?.string.value;
        if (std.mem.eql(u8, name, "Alice")) {
            try testing.expect(!seen_alice);
            seen_alice = true;
        } else if (std.mem.eql(u8, name, "Bob")) {
            try testing.expect(!seen_bob);
            seen_bob = true;
        } else {
            try testing.expect(false);
        }
        count += 1;
    }
    try testing.expectEqual(@as(usize, 2), count);
    try testing.expect(seen_alice and seen_bob);
}

test "Bucket.explicit $and query uses range planning" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = std.testing.allocator;
    platform.deleteFile("index-query-explicit-and-range.bucket") catch {};
    var bucket = try Bucket.init(allocator, "index-query-explicit-and-range.bucket");
    try setupIndexQueryBucket(&bucket, allocator);
    defer bucket.deinit();
    defer platform.deleteFile("index-query-explicit-and-range.bucket") catch {};

    var qdoc = try bson.fmt.serialize(.{
        .query = .{
            .@"$and" = .{
                .{ .age = .{ .@"$gte" = 35 } },
                .{ .age = .{ .@"$lte" = 40 } },
            },
        },
    }, allocator);
    defer qdoc.deinit(allocator);

    var q = try query.Query.parse(allocator, qdoc);
    defer q.deinit(allocator);

    const plan = bucket.planQuery(&q);
    try testing.expect(plan.source == .index);
    try testing.expect(plan.index_strategy == Bucket.QueryPlan.IndexStrategy.range);
    try testing.expect(plan.index_path != null and std.mem.eql(u8, plan.index_path.?, "age"));
    try testing.expect(plan.bounds.lower != null);
    try testing.expect(plan.bounds.upper != null);

    var iter = try bucket.listIterate(&arena, q);
    defer iter.deinit() catch {};
    var seen_bob = false;
    var seen_dora = false;
    var count: usize = 0;
    while (try iter.next(iter)) |doc| {
        const name = doc.get("name").?.string.value;
        if (std.mem.eql(u8, name, "Bob")) {
            seen_bob = true;
        } else if (std.mem.eql(u8, name, "Dora")) {
            seen_dora = true;
        } else {
            try testing.expect(false);
        }
        count += 1;
    }
    try testing.expectEqual(@as(usize, 2), count);
    try testing.expect(seen_bob and seen_dora);
}

test "Bucket.explicit $and query uses points planning" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = std.testing.allocator;
    platform.deleteFile("index-query-explicit-and-points.bucket") catch {};
    var bucket = try Bucket.init(allocator, "index-query-explicit-and-points.bucket");
    try setupIndexQueryBucket(&bucket, allocator);
    defer bucket.deinit();
    defer platform.deleteFile("index-query-explicit-and-points.bucket") catch {};

    var qdoc = try bson.fmt.serialize(.{
        .query = .{
            .@"$and" = .{
                .{ .age = .{ .@"$in" = [_]i32{ 30, 40 } } },
                .{ .name = .{ .@"$ne" = "Bob" } },
            },
        },
    }, allocator);
    defer qdoc.deinit(allocator);

    var q = try query.Query.parse(allocator, qdoc);
    defer q.deinit(allocator);

    const plan = bucket.planQuery(&q);
    try testing.expect(plan.source == .index);
    try testing.expect(plan.index_strategy == Bucket.QueryPlan.IndexStrategy.points);
    try testing.expect(plan.index_path != null and std.mem.eql(u8, plan.index_path.?, "age"));

    var iter = try bucket.listIterate(&arena, q);
    defer iter.deinit() catch {};
    var count: usize = 0;
    while (try iter.next(iter)) |doc| {
        try testing.expectEqualStrings("Alice", doc.get("name").?.string.value);
        count += 1;
    }
    try testing.expectEqual(@as(usize, 1), count);
}

test "Bucket.indexed query between uses range" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = std.testing.allocator;
    platform.deleteFile("index-query-between.bucket") catch {};
    var bucket = try Bucket.init(allocator, "index-query-between.bucket");
    try setupIndexQueryBucket(&bucket, allocator);
    defer bucket.deinit();
    defer platform.deleteFile("index-query-between.bucket") catch {};

    var qdoc = try bson.fmt.serialize(.{ .query = .{ .age = .{ .@"$between" = [_]i32{ 29, 38 } } } }, allocator);
    defer qdoc.deinit(allocator);

    var q = try query.Query.parse(allocator, qdoc);
    defer q.deinit(allocator);

    const plan = bucket.planQuery(&q);
    try testing.expect(plan.source == .index);
    try testing.expect(plan.index_strategy == Bucket.QueryPlan.IndexStrategy.range);

    var iter = try bucket.listIterate(&arena, q);
    defer iter.deinit() catch {};
    var seen_alice = false;
    var seen_dora = false;
    var count: usize = 0;
    while (try iter.next(iter)) |doc| {
        const name = doc.get("name").?.string.value;
        if (std.mem.eql(u8, name, "Alice")) {
            seen_alice = true;
        } else if (std.mem.eql(u8, name, "Dora")) {
            seen_dora = true;
        } else {
            try testing.expect(false);
        }
        count += 1;
    }
    try testing.expectEqual(@as(usize, 2), count);
    try testing.expect(seen_alice and seen_dora);
}

test "Bucket.indexed query $nor uses exclusion planning" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = std.testing.allocator;
    platform.deleteFile("index-query-nor.bucket") catch {};
    var bucket = try Bucket.init(allocator, "index-query-nor.bucket");
    try setupIndexQueryBucket(&bucket, allocator);
    defer bucket.deinit();
    defer platform.deleteFile("index-query-nor.bucket") catch {};

    var qdoc = try bson.fmt.serialize(.{
        .query = .{
            .@"$nor" = .{
                .{ .age = 30 },
                .{ .age = 40 },
            },
        },
    }, allocator);
    defer qdoc.deinit(allocator);

    var q = try query.Query.parse(allocator, qdoc);
    defer q.deinit(allocator);

    const plan = bucket.planQuery(&q);
    try testing.expect(plan.source == .index);
    try testing.expect(plan.index_strategy == Bucket.QueryPlan.IndexStrategy.nor_exclusion);
    try testing.expect(plan.eager);
    try testing.expect(plan.index_path == null);

    var docs: std.ArrayList(BSONDocument) = .{};
    defer docs.deinit(allocator);
    try bucket.collectDocs(&docs, allocator, &q, &plan);

    var seen_carol = false;
    var seen_dora = false;
    for (docs.items) |doc| {
        const name = doc.get("name").?.string.value;
        if (std.mem.eql(u8, name, "Carol")) {
            seen_carol = true;
        } else if (std.mem.eql(u8, name, "Dora")) {
            seen_dora = true;
        } else {
            try testing.expect(false);
        }
    }
    try testing.expectEqual(@as(usize, 2), docs.items.len);
    try testing.expect(seen_carol and seen_dora);
}

test "Bucket.indexed query $nor still uses exclusion without _id index" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = std.testing.allocator;
    var bucket = try Bucket.init(allocator, ":memory:");
    try setupIndexQueryBucket(&bucket, allocator);
    defer bucket.deinit();

    try bucket.dropIndex("_id");
    try testing.expect(!bucket.indexes.contains("_id"));

    var qdoc = try bson.fmt.serialize(.{
        .query = .{
            .@"$nor" = .{
                .{ .age = 30 },
                .{ .age = 40 },
            },
        },
    }, allocator);
    defer qdoc.deinit(allocator);

    var q = try query.Query.parse(allocator, qdoc);
    defer q.deinit(allocator);

    const plan = bucket.planQuery(&q);
    try testing.expect(plan.index_strategy == Bucket.QueryPlan.IndexStrategy.nor_exclusion);

    var docs: std.ArrayList(BSONDocument) = .{};
    defer docs.deinit(allocator);
    try bucket.collectDocs(&docs, allocator, &q, &plan);

    var seen_carol = false;
    var seen_dora = false;
    for (docs.items) |doc| {
        const name = doc.get("name").?.string.value;
        if (std.mem.eql(u8, name, "Carol")) {
            seen_carol = true;
        } else if (std.mem.eql(u8, name, "Dora")) {
            seen_dora = true;
        } else {
            try testing.expect(false);
        }
    }
    try testing.expectEqual(@as(usize, 2), docs.items.len);
    try testing.expect(seen_carol and seen_dora);
}

test "Bucket.indexed query in scores avoids duplicates" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = std.testing.allocator;
    platform.deleteFile("index-query-scores.bucket") catch {};
    var bucket = try Bucket.init(allocator, "index-query-scores.bucket");
    try setupIndexQueryBucket(&bucket, allocator);
    defer bucket.deinit();
    defer platform.deleteFile("index-query-scores.bucket") catch {};

    var qdoc = try bson.fmt.serialize(.{ .query = .{ .scores = .{ .@"$in" = [_]i32{7} } } }, allocator);
    defer qdoc.deinit(allocator);

    var q = try query.Query.parse(allocator, qdoc);
    defer q.deinit(allocator);

    const plan = bucket.planQuery(&q);
    try testing.expect(plan.source == .index);
    try testing.expect(plan.index_strategy == Bucket.QueryPlan.IndexStrategy.points);
    try testing.expect(plan.index_path != null and std.mem.eql(u8, plan.index_path.?, "scores"));

    var doc_list: std.ArrayList(BSONDocument) = .{};
    defer doc_list.deinit(allocator);
    try bucket.collectDocs(&doc_list, allocator, &q, &plan);

    var seen_alice = false;
    var seen_dora = false;
    for (doc_list.items) |doc| {
        const name = doc.get("name").?.string.value;
        if (std.mem.eql(u8, name, "Alice")) {
            try testing.expect(!seen_alice);
            seen_alice = true;
        } else if (std.mem.eql(u8, name, "Dora")) {
            try testing.expect(!seen_dora);
            seen_dora = true;
        } else {
            try testing.expect(false);
        }
    }
    try testing.expectEqual(@as(usize, 2), doc_list.items.len);
    try testing.expect(seen_alice and seen_dora);
}

test "Page cache enforces capacity with LRU eviction" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();
    const bucket_path = "cache-capacity.bucket";

    var bucket = try Bucket.openFileWithOptions(allocator, bucket_path, .{
        .page_cache_capacity = 2,
    });
    defer bucket.deinit();
    defer platform.deleteFile(bucket_path) catch {
        // std.debug.print("Failed to delete test file: {any}\n", .{err});
    };

    _ = try bucket.loadPage(0); // meta page
    _ = try bucket.createNewPage(.Data);
    _ = try bucket.createNewPage(.Data);
    _ = try bucket.createNewPage(.Data);

    try testing.expectEqual(@as(usize, 2), bucket.pageCache.map.count());
    var found_key0 = false;
    var found_key2 = false;
    var found_key3 = false;
    var iter = bucket.pageCache.map.iterator();
    while (iter.next()) |entry| {
        const key = entry.key_ptr.*;
        if (key == 0) found_key0 = true;
        if (key == 3) found_key2 = true;
        if (key == 4) found_key3 = true;
    }
    try testing.expect(!found_key0);
    try testing.expect(found_key2);
    try testing.expect(found_key3);
}

test "Page overflow" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    const allocator = arena.allocator();
    defer arena.deinit();
    var bucket = try Bucket.openFile(allocator, "overflow.bucket");
    defer bucket.deinit();
    defer platform.deleteFile("overflow.bucket") catch {
        // std.debug.print("Failed to delete test file: {any}\n", .{err});
    };
    for (0..900) |i| {
        const name = try std.fmt.allocPrint(allocator, "test-{d}", .{i});
        const docMany = try bson.fmt.serialize(.{ .name = name, .age = 10 }, allocator);

        _ = try bucket.insert(docMany);
        allocator.free(name);
        allocator.free(docMany.buffer);
    }

    var q1Doc = try bson.fmt.serialize(.{ .sector = .{} }, allocator);
    defer q1Doc.deinit(allocator);
    const q1 = try query.Query.parse(allocator, q1Doc);

    var res3list = std.ArrayList(BSONDocument){};
    defer res3list.deinit(allocator);
    var res3iter = try bucket.listIterate(&arena, q1);
    while (try res3iter.next(res3iter)) |doc| {
        try res3list.append(allocator, doc);
    }
    const res3 = res3list.items;

    // std.debug.print("Documents in bucket: {d}\n", .{res3.len});
    try testing.expect(res3.len == 900);
}

test "Bucket.delete" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();
    var bucket = try Bucket.init(allocator, "DELETE.bucket");
    defer bucket.deinit();
    defer platform.deleteFile("DELETE.bucket") catch {
        // std.debug.print("Failed to delete test file: {any}\n", .{err});
    };

    // Create a new BSON document
    var doc = try bson.fmt.serialize(.{ .name = "Alice", .age = "delete me" }, allocator);
    defer doc.deinit(allocator);

    // Insert the document into the bucket
    _ = try bucket.insert(doc);

    var listQDoc = try bson.fmt.serialize(.{ .query = .{} }, allocator);
    defer listQDoc.deinit(allocator);
    const listQ = try query.Query.parse(allocator, listQDoc);

    var docsList = std.ArrayList(BSONDocument){};
    defer docsList.deinit(allocator);
    var docsIter = try bucket.listIterate(&arena, listQ);
    while (try docsIter.next(docsIter)) |docItem| {
        try docsList.append(allocator, docItem);
    }
    const docs = docsList.items;
    const docCount = docs.len;

    // std.debug.print("Doc len before vacuum {d}\n", .{docCount});
    try std.testing.expect(docCount == 1);

    var deleteQDoc = try bson.fmt.serialize(.{ .query = .{ .name = "Alice" } }, allocator);
    defer deleteQDoc.deinit(allocator);
    const deleteQ = try query.Query.parse(allocator, deleteQDoc);
    // Delete the document from the bucket
    _ = try bucket.delete(deleteQ);

    var newDocsList = std.ArrayList(BSONDocument){};
    defer newDocsList.deinit(allocator);
    var newDocsIter = try bucket.listIterate(&arena, listQ);
    while (try newDocsIter.next(newDocsIter)) |docItem| {
        try newDocsList.append(allocator, docItem);
    }
    const newDocs = newDocsList.items;
    const newDocCount = newDocs.len;
    // std.debug.print("Doc len after delete {d}\n", .{newDocCount});

    try std.testing.expect(newDocCount < docCount);

    _ = try bucket.insert(doc);

    var afterInsertList = std.ArrayList(BSONDocument){};
    defer afterInsertList.deinit(allocator);
    var afterInsertIter = try bucket.listIterate(&arena, listQ);
    while (try afterInsertIter.next(afterInsertIter)) |docItem| {
        try afterInsertList.append(allocator, docItem);
    }
    const docsAfterInsert = afterInsertList.items;
    const docCountAfterInsert = docsAfterInsert.len;
    // std.debug.print("Doc len after insert {d}\n", .{docCountAfterInsert});
    try std.testing.expect(docCountAfterInsert == docCount);
    _ = try bucket.delete(listQ);
}

test "Deleted docs disappear after vacuum" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();
    var bucket = try Bucket.init(allocator, "VACUUM.bucket");
    defer bucket.deinit();
    defer platform.deleteFile("VACUUM.bucket") catch |err| {
        std.debug.print("Failed to delete test file: {any}\n", .{err});
    };

    // Create a new BSON document
    var doc = try bson.fmt.serialize(.{ .name = "Alice", .age = "delete me" }, allocator);
    defer doc.deinit(allocator);

    var docGood = try bson.fmt.serialize(.{ .name = "not Alice", .age = "delete me" }, allocator);
    defer docGood.deinit(allocator);

    // Insert the document into the bucket
    _ = try bucket.insert(doc);
    _ = try bucket.insert(docGood);

    var listQDoc = try bson.fmt.serialize(.{ .query = .{} }, allocator);
    defer listQDoc.deinit(allocator);
    const listQ = try query.Query.parse(allocator, listQDoc);

    var docsList = std.ArrayList(BSONDocument){};
    defer docsList.deinit(allocator);
    var docsIter = try bucket.listIterate(&arena, listQ);
    while (try docsIter.next(docsIter)) |docItem| {
        try docsList.append(allocator, docItem);
    }

    var deleteQDoc = try bson.fmt.serialize(.{ .query = .{ .name = "Alice" } }, allocator);
    defer deleteQDoc.deinit(allocator);
    const deleteQ = try query.Query.parse(allocator, deleteQDoc);

    bucket.delete(deleteQ) catch |err| {
        std.debug.print("Failed to delete document: {any}\n", .{err});
    };

    bucket.vacuum() catch |err| {
        std.debug.print("Failed to vacuum bucket: {any}\n", .{err});
        return err;
    };

    var afterVacuumList = std.ArrayList(BSONDocument){};
    defer afterVacuumList.deinit(allocator);
    var afterVacuumIter = try bucket.listIterate(&arena, listQ);
    while (try afterVacuumIter.next(afterVacuumIter)) |docItem| {
        try afterVacuumList.append(allocator, docItem);
    }
    const afterVacuumDocs = afterVacuumList.items;
    const afterVacuumDocCount = afterVacuumDocs.len;

    try std.testing.expect(afterVacuumDocCount == 1);
}

test "Bucket.dropIndex removes metadata entry" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();
    const file_name = "drop-index.bucket";
    defer platform.deleteFile(file_name) catch |err| {
        std.debug.print("Failed to delete test file: {any}\n", .{err});
    };

    {
        var bucket = try Bucket.init(allocator, file_name);
        defer bucket.deinit();

        try bucket.ensureIndex("age", .{});
        try testing.expect(bucket.indexes.contains("age"));

        try testing.expectError(error.IndexNotFound, bucket.dropIndex("missing"));

        try bucket.dropIndex("age");
        try testing.expect(!bucket.indexes.contains("age"));

        try testing.expectError(error.IndexNotFound, bucket.dropIndex("age"));
    }

    {
        var reopened = try Bucket.init(allocator, file_name);
        defer reopened.deinit();

        try testing.expect(!reopened.indexes.contains("age"));
        try testing.expect(reopened.indexes.contains("_id"));
    }
}

test "Bucket.reverse index with sort queries" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();
    var bucket = try Bucket.init(allocator, "reverse-index-sort.bucket");
    defer bucket.deinit();
    defer platform.deleteFile("reverse-index-sort.bucket") catch {};

    // Create a reverse index on the "score" field
    try bucket.ensureIndex("score", .{ .reverse = 1 });

    // Insert documents with different scores
    const test_docs = [_]struct { name: []const u8, score: i32 }{
        .{ .name = "Alice", .score = 10 },
        .{ .name = "Bob", .score = 50 },
        .{ .name = "Carol", .score = 30 },
        .{ .name = "Dave", .score = 40 },
        .{ .name = "Eve", .score = 20 },
    };

    for (test_docs) |td| {
        var doc = try bson.fmt.serialize(td, allocator);
        _ = try bucket.insert(doc);
        doc.deinit(allocator);
    }

    // Test 1: Query with descending sort should use the reverse index (sort_covered = true)
    {
        var query_doc = try bson.fmt.serialize(.{ .sort = .{ .desc = "score" } }, allocator);
        defer query_doc.deinit(allocator);

        var q = try query.Query.parse(allocator, query_doc);
        defer q.deinit(allocator);

        const plan = bucket.planQuery(&q);
        try testing.expect(plan.sort_covered); // Reverse index should cover descending sort
        try testing.expect(plan.source == .index);
        try testing.expectEqualStrings("score", plan.index_path.?);

        var iter = try bucket.listIterate(&arena, q);
        defer iter.deinit() catch {};

        // Should return in descending order: Bob(50), Dave(40), Carol(30), Eve(20), Alice(10)
        const expected_scores = [_]i32{ 50, 40, 30, 20, 10 };
        var idx: usize = 0;
        while (try iter.next(iter)) |doc| {
            const score = doc.get("score").?.int32.value;
            try testing.expectEqual(expected_scores[idx], score);
            idx += 1;
            // allocator.free(doc.buffer);
        }
        try testing.expectEqual(@as(usize, 5), idx);
    }

    // Test 2: Query with ascending sort should NOT use reverse index efficiently (sort_covered = false)
    {
        var query_doc = try bson.fmt.serialize(.{ .sort = .{ .asc = "score" } }, allocator);
        defer query_doc.deinit(allocator);

        var q = try query.Query.parse(allocator, query_doc);
        defer q.deinit(allocator);

        const plan = bucket.planQuery(&q);
        try testing.expect(!plan.sort_covered); // Reverse index doesn't cover ascending sort
        // Results should still be correct due to manual sorting

        var iter = try bucket.listIterate(&arena, q);
        defer iter.deinit() catch {};

        // Should return in ascending order: Alice(10), Eve(20), Carol(30), Dave(40), Bob(50)
        const expected_scores = [_]i32{ 10, 20, 30, 40, 50 };
        var idx: usize = 0;
        while (try iter.next(iter)) |doc| {
            const score = doc.get("score").?.int32.value;
            try testing.expectEqual(expected_scores[idx], score);
            idx += 1;
            // allocator.free(doc.buffer);
        }
        try testing.expectEqual(@as(usize, 5), idx);
    }

    // Test 3: Range query with descending sort on reverse index
    {
        var query_doc = try bson.fmt.serialize(
            .{
                .query = .{ .score = .{ .@"$gte" = 20, .@"$lte" = 40 } },
                .sort = .{ .desc = "score" },
            },
            allocator,
        );
        defer query_doc.deinit(allocator);

        var q = try query.Query.parse(allocator, query_doc);
        defer q.deinit(allocator);

        const plan = bucket.planQuery(&q);
        try testing.expect(plan.sort_covered); // Should be covered by reverse index
        try testing.expect(plan.source == .index);
        try testing.expect(plan.index_strategy == Bucket.QueryPlan.IndexStrategy.range);

        var iter = try bucket.listIterate(&arena, q);
        defer iter.deinit() catch {};

        // Should return in descending order: Dave(40), Carol(30), Eve(20)
        const expected_scores = [_]i32{ 40, 30, 20 };
        var idx: usize = 0;
        while (try iter.next(iter)) |doc| {
            const score = doc.get("score").?.int32.value;
            try testing.expectEqual(expected_scores[idx], score);
            idx += 1;
            allocator.free(doc.buffer);
        }
        try testing.expectEqual(@as(usize, 3), idx);
    }
}

test "Bucket.replication with WAL batches" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const primary_path = "replication-primary.bucket";
    const primary_wal_path = std.fmt.comptimePrint("{s}-wal", .{primary_path});
    const primary_shm_path = std.fmt.comptimePrint("{s}-wal-shm", .{primary_path});
    const replica_path = "replication-replica.bucket";
    const replica_wal_path = std.fmt.comptimePrint("{s}-wal", .{replica_path});
    const replica_shm_path = std.fmt.comptimePrint("{s}-wal-shm", .{replica_path});

    platform.deleteFile(primary_path) catch {};
    std.fs.cwd().deleteFile(primary_wal_path) catch {};
    std.fs.cwd().deleteFile(primary_shm_path) catch {};
    platform.deleteFile(replica_path) catch {};
    std.fs.cwd().deleteFile(replica_wal_path) catch {};
    std.fs.cwd().deleteFile(replica_shm_path) catch {};
    defer platform.deleteFile(primary_path) catch {};
    defer std.fs.cwd().deleteFile(primary_wal_path) catch {};
    defer std.fs.cwd().deleteFile(primary_shm_path) catch {};
    defer platform.deleteFile(replica_path) catch {};
    defer std.fs.cwd().deleteFile(replica_wal_path) catch {};
    defer std.fs.cwd().deleteFile(replica_shm_path) catch {};

    var primary = try Bucket.openFileWithOptions(allocator, primary_path, .{ .wal = true });
    defer primary.deinit();
    var replica = try Bucket.openFileWithOptions(allocator, replica_path, .{ .wal = true });
    defer replica.deinit();

    const Person = struct { name: []const u8, age: i32 };
    const initial_cursor = try primary.replicationCursor();

    // Insert documents into primary
    const test_docs = [_]Person{
        .{ .name = "Alice", .age = 30 },
        .{ .name = "Bob", .age = 25 },
        .{ .name = "Carol", .age = 35 },
    };

    for (test_docs) |td| {
        var doc = try bson.fmt.serialize(td, allocator);
        _ = try primary.insert(doc);
        doc.deinit(allocator);
    }

    // Force flush to trigger replication
    try primary.flush();

    const batch1 = (try primary.readReplicationBatch(initial_cursor, 0, allocator)) orelse return error.TestExpectedEqual;
    defer allocator.free(batch1);
    const replica_cursor1 = try replica.applyReplicationBatch(batch1);
    const primary_cursor1 = try primary.replicationCursor();
    try testing.expectEqual(primary_cursor1.generation, replica_cursor1.generation);
    try testing.expectEqual(primary_cursor1.next_frame_index, replica_cursor1.next_frame_index);
    try testing.expect((try primary.readReplicationBatch(primary_cursor1, 0, allocator)) == null);

    // Clear replica's cache to force reading from disk
    replica.pageCache.clear(allocator);

    // Query replica to verify data was replicated
    var query_doc = try bson.fmt.serialize(.{ .sector = .{} }, allocator);
    defer query_doc.deinit(allocator);

    var q = try query.Query.parse(allocator, query_doc);
    defer q.deinit(allocator);

    var iter = try replica.listIterate(&arena, q);
    defer iter.deinit() catch {};

    var docs_found: usize = 0;
    var alice_found = false;
    var bob_found = false;
    var carol_found = false;

    while (try iter.next(iter)) |doc| {
        const parseDoc = try bson.fmt.parse(Person, doc, allocator);

        const name = parseDoc.value.name;

        // std.debug.print("Found in replica: {s}, age: {d}\n", .{ name, age });

        // Check names before freeing the buffer
        if (std.mem.eql(u8, name, "Alice")) alice_found = true;
        if (std.mem.eql(u8, name, "Bob")) bob_found = true;
        if (std.mem.eql(u8, name, "Carol")) carol_found = true;

        docs_found += 1;
        allocator.free(doc.buffer);
    }

    // Verify all documents were replicated
    try testing.expectEqual(@as(usize, 3), docs_found);
    try testing.expect(alice_found);
    try testing.expect(bob_found);
    try testing.expect(carol_found);

    // Test incremental replication: insert more docs
    const more_docs = [_]Person{
        .{ .name = "Dave", .age = 28 },
        .{ .name = "Eve", .age = 32 },
    };

    for (more_docs) |td| {
        var doc = try bson.fmt.serialize(td, allocator);
        _ = try primary.insert(doc);
        doc.deinit(allocator);
    }

    try primary.flush();

    const batch2 = (try primary.readReplicationBatch(replica_cursor1, 0, allocator)) orelse return error.TestExpectedEqual;
    defer allocator.free(batch2);
    const replica_cursor2 = try replica.applyReplicationBatch(batch2);
    const primary_cursor2 = try primary.replicationCursor();
    try testing.expectEqual(primary_cursor2.next_frame_index, replica_cursor2.next_frame_index);

    // Idempotent replay of the same batch is a no-op.
    const replay_cursor = try replica.applyReplicationBatch(batch2);
    try testing.expectEqual(replica_cursor2.next_frame_index, replay_cursor.next_frame_index);

    // Clear cache again
    replica.pageCache.clear(allocator);

    // Query replica again to verify all 5 documents exist
    var iter2 = try replica.listIterate(&arena, q);
    defer iter2.deinit() catch {};

    docs_found = 0;
    while (try iter2.next(iter2)) |doc| {
        docs_found += 1;
        allocator.free(doc.buffer);
    }

    try testing.expectEqual(@as(usize, 5), docs_found);
}

test "Deleting removes index entries for scalar field" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const file_name = "index-delete.bucket";
    var bucket = try Bucket.init(allocator, file_name);
    defer bucket.deinit();
    defer platform.deleteFile(file_name) catch {};

    try bucket.ensureIndex("age", .{});

    // Insert two docs; only one matches age = 42
    var doc_a = try bson.fmt.serialize(.{ .name = "Zed", .age = 42 }, allocator);
    _ = try bucket.insert(doc_a);
    doc_a.deinit(allocator);

    var doc_b = try bson.fmt.serialize(.{ .name = "Other", .age = 30 }, allocator);
    _ = try bucket.insert(doc_b);
    doc_b.deinit(allocator);

    // Build equality query on age to ensure index plan
    var qdoc = try bson.fmt.serialize(.{ .query = .{ .age = 42 } }, allocator);
    defer qdoc.deinit(allocator);
    var q = try query.Query.parse(allocator, qdoc);
    defer q.deinit(allocator);

    var plan = bucket.planQuery(&q);
    try testing.expect(plan.source == .index);
    try testing.expect(plan.index_path != null and std.mem.eql(u8, plan.index_path.?, "age"));

    // Count index entries before delete (directly via index iterator)
    var iter_before = try bucket.initIndexIterator(&plan);
    var count_before: usize = 0;
    while (try iter_before.next()) |_| {
        count_before += 1;
    }
    try testing.expectEqual(@as(usize, 1), count_before);

    // Delete the matching document by name
    var del_qdoc = try bson.fmt.serialize(.{ .query = .{ .name = "Zed" } }, allocator);
    defer del_qdoc.deinit(allocator);
    var del_q = try query.Query.parse(allocator, del_qdoc);
    defer del_q.deinit(allocator);
    try bucket.delete(del_q);

    // Rebuild plan and iterate index again; it should yield zero entries
    plan = bucket.planQuery(&q);
    try testing.expect(plan.source == .index);
    var iter_after = try bucket.initIndexIterator(&plan);
    var count_after: usize = 0;
    while (try iter_after.next()) |_| {
        count_after += 1;
    }
    try testing.expectEqual(@as(usize, 0), count_after);
}

test "Bucket clean close invalidates saved WAL generation" {
    const allocator = std.testing.allocator;
    const db_path = "albedo_test_wal_restore.bucket";
    const wal_path = "albedo_test_wal_restore.bucket-wal";
    const shm_path = "albedo_test_wal_restore.bucket-wal-shm";

    // Clean up any leftovers from previous runs.
    platform.deleteFile(db_path) catch {};
    std.fs.cwd().deleteFile(wal_path) catch {};
    std.fs.cwd().deleteFile(shm_path) catch {};
    defer platform.deleteFile(db_path) catch {};
    defer std.fs.cwd().deleteFile(wal_path) catch {};
    defer std.fs.cwd().deleteFile(shm_path) catch {};

    // ── Phase 1: create bucket, insert documents, and save the WAL ──

    // Record the DB file size *before* inserting docs so we can truncate back.
    var saved_wal_bytes: []u8 = undefined;
    var pre_insert_db_size: u64 = undefined;

    {
        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();
        const ally = arena.allocator();

        var bucket = try Bucket.init(ally, db_path);

        // After init + meta page write, grab pre-insert file size.
        try bucket.flush();
        const db_stat = try std.fs.cwd().statFile(db_path);
        pre_insert_db_size = db_stat.size;

        // Insert three documents.
        const doc1 = try bson.fmt.serialize(.{ .name = "Alice", .age = @as(i32, 30) }, ally);
        _ = try bucket.insert(doc1);
        const doc2 = try bson.fmt.serialize(.{ .name = "Bob", .age = @as(i32, 25) }, ally);
        _ = try bucket.insert(doc2);
        const doc3 = try bson.fmt.serialize(.{ .name = "Charlie", .age = @as(i32, 40) }, ally);
        _ = try bucket.insert(doc3);

        // Flush to ensure WAL has committed frames.
        try bucket.flush();
        if (bucket.wal) |*w| {
            try w.sync();
        }

        // Verify we can read docs back before the crash.
        try testing.expectEqual(@as(u64, 3), bucket.header.doc_count);

        // Save WAL bytes to a heap buffer before deinit consumes it.
        const wal_file = try std.fs.cwd().openFile(wal_path, .{});
        defer wal_file.close();
        const wal_stat = try wal_file.stat();
        saved_wal_bytes = try allocator.alloc(u8, wal_stat.size);
        const n = try wal_file.readAll(saved_wal_bytes);
        try testing.expect(n == saved_wal_bytes.len);

        // Normal close — this consumes the WAL.
        bucket.deinit();
    }
    defer allocator.free(saved_wal_bytes);

    // ── Phase 2: simulate a crash ──────────────────────────────────

    // Truncate the main DB file back to its pre-insert size (lose data pages).
    {
        const db_file = try std.fs.cwd().openFile(db_path, .{ .mode = .read_write });
        defer db_file.close();
        try db_file.setEndPos(pre_insert_db_size);
        try db_file.sync();
    }

    // Restore the saved WAL (as if the WAL survived the crash).
    {
        const wal_file = try std.fs.cwd().createFile(wal_path, .{});
        defer wal_file.close();
        try wal_file.writeAll(saved_wal_bytes);
        try wal_file.sync();
    }

    // ── Phase 3: reopen — stale WAL generation should be rejected ─

    {
        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();
        const ally = arena.allocator();

        try testing.expectError(BucketInitErrors.InitializationError, Bucket.openFile(ally, db_path));
    }
}

test "Bucket preserves WAL history when replication generation is exhausted" {
    const allocator = std.testing.allocator;
    const db_path = "albedo_test_replication_generation_exhausted.bucket";
    const wal_path = "albedo_test_replication_generation_exhausted.bucket-wal";
    const shm_path = "albedo_test_replication_generation_exhausted.bucket-wal-shm";

    platform.deleteFile(db_path) catch {};
    std.fs.cwd().deleteFile(wal_path) catch {};
    std.fs.cwd().deleteFile(shm_path) catch {};
    defer platform.deleteFile(db_path) catch {};
    defer std.fs.cwd().deleteFile(wal_path) catch {};
    defer std.fs.cwd().deleteFile(shm_path) catch {};

    {
        var bucket = try Bucket.openFileWithOptions(allocator, db_path, .{ .wal = true });
        bucket.header.replication_generation = std.math.maxInt(u64);
        bucket.persistHeaderToMainFile();
        if (bucket.wal) |*w| {
            try w.setGeneration(std.math.maxInt(u64));
        }

        var doc = try bson.fmt.serialize(.{ .name = "Alice" }, allocator);
        defer doc.deinit(allocator);
        _ = try bucket.insert(doc);
        try bucket.flush();
        bucket.deinit();
    }

    try std.fs.cwd().access(wal_path, .{});
    try std.fs.cwd().access(shm_path, .{});

    {
        var reopened = try Bucket.openFileWithOptions(allocator, db_path, .{ .wal = true });
        defer reopened.deinit();

        try testing.expectEqual(std.math.maxInt(u64), reopened.header.replication_generation);
        try testing.expectEqual(@as(u64, 1), reopened.header.doc_count);
    }
}

test "Bucket.vacuum fails when replication generation is exhausted" {
    const allocator = std.testing.allocator;
    const db_path = "albedo_test_vacuum_generation_exhausted.bucket";
    const wal_path = "albedo_test_vacuum_generation_exhausted.bucket-wal";
    const shm_path = "albedo_test_vacuum_generation_exhausted.bucket-wal-shm";

    platform.deleteFile(db_path) catch {};
    std.fs.cwd().deleteFile(wal_path) catch {};
    std.fs.cwd().deleteFile(shm_path) catch {};
    defer platform.deleteFile(db_path) catch {};
    defer std.fs.cwd().deleteFile(wal_path) catch {};
    defer std.fs.cwd().deleteFile(shm_path) catch {};

    var bucket = try Bucket.openFileWithOptions(allocator, db_path, .{ .wal = true });
    defer bucket.deinit();

    bucket.header.replication_generation = std.math.maxInt(u64);
    bucket.persistHeaderToMainFile();
    if (bucket.wal) |*w| {
        try w.setGeneration(std.math.maxInt(u64));
    }

    try testing.expectError(error.ReplicationGenerationExhausted, bucket.vacuum());
}

test "Bucket WAL replays frames after abrupt crash (no clean shutdown)" {
    const allocator = std.testing.allocator;
    const db_path = "albedo_test_wal_crash.bucket";
    const wal_path = "albedo_test_wal_crash.bucket-wal";
    const shm_path = "albedo_test_wal_crash.bucket-wal-shm";

    // Clean up any leftovers from previous runs.
    platform.deleteFile(db_path) catch {};
    std.fs.cwd().deleteFile(wal_path) catch {};
    std.fs.cwd().deleteFile(shm_path) catch {};
    defer platform.deleteFile(db_path) catch {};
    defer std.fs.cwd().deleteFile(wal_path) catch {};
    defer std.fs.cwd().deleteFile(shm_path) catch {};

    // ── Phase 1: create bucket, insert documents, then crash ──────

    var pre_insert_db_size: u64 = undefined;

    {
        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();
        const ally = arena.allocator();

        var bucket = try Bucket.init(ally, db_path);

        // Grab the file size before any inserts.
        try bucket.flush();
        const db_stat = try std.fs.cwd().statFile(db_path);
        pre_insert_db_size = db_stat.size;

        // Insert documents.
        const doc1 = try bson.fmt.serialize(.{ .title = "Doc A", .value = @as(i32, 1) }, ally);
        _ = try bucket.insert(doc1);
        const doc2 = try bson.fmt.serialize(.{ .title = "Doc B", .value = @as(i32, 2) }, ally);
        _ = try bucket.insert(doc2);

        // Ensure WAL frames are committed to the WAL file.
        try bucket.flush();
        if (bucket.wal) |*w| {
            try w.sync();
        }

        try testing.expectEqual(@as(u64, 2), bucket.header.doc_count);

        // ── Simulate abrupt crash ──
        // Close WAL file descriptors without consuming (leaves WAL on disk).
        if (bucket.wal) |*w| {
            w.deinit();
            bucket.wal = null;
        }
        // Close the rest of the bucket normally.
        bucket.deinit();
    }

    // The WAL file should still exist on disk after the "crash".
    _ = try std.fs.cwd().statFile(wal_path);

    // Truncate the main DB file to lose the data pages written after init.
    {
        const db_file = try std.fs.cwd().openFile(db_path, .{ .mode = .read_write });
        defer db_file.close();
        try db_file.setEndPos(pre_insert_db_size);
        try db_file.sync();
    }

    // ── Phase 2: reopen — WAL replay should restore everything ────

    {
        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();
        const ally = arena.allocator();

        var bucket = try Bucket.openFile(ally, db_path);
        defer bucket.deinit();

        // After replay the header should reflect both documents.
        try testing.expectEqual(@as(u64, 2), bucket.header.doc_count);

        // Query all documents.
        var qDoc = try bson.fmt.serialize(.{ .sector = .{} }, ally);
        defer qDoc.deinit(ally);
        const q = try query.Query.parse(ally, qDoc);

        const docs = try bucket.list(ally, q);
        try testing.expectEqual(@as(usize, 2), docs.len);

        var found_a = false;
        var found_b = false;
        for (docs) |d| {
            if (d.get("title")) |v| {
                const title = v.string.value;
                if (std.mem.eql(u8, title, "Doc A")) found_a = true;
                if (std.mem.eql(u8, title, "Doc B")) found_b = true;
            }
        }
        try testing.expect(found_a);
        try testing.expect(found_b);
    }

    // After the clean reopen + deinit the WAL should have been consumed.
    if (std.fs.cwd().statFile(wal_path)) |_| {
        return error.TestUnexpectedResult; // WAL should be gone after clean close
    } else |_| {}
}

test "WAL live-tail: reader polls for docs written by another connection" {
    const allocator = std.testing.allocator;
    const db_path = "albedo_test_wal_live_tail.bucket";
    const wal_path = "albedo_test_wal_live_tail.bucket-wal";
    const shm_path = "albedo_test_wal_live_tail.bucket-wal-shm";

    // Clean up leftovers.
    platform.deleteFile(db_path) catch {};
    std.fs.cwd().deleteFile(wal_path) catch {};
    std.fs.cwd().deleteFile(shm_path) catch {};
    defer platform.deleteFile(db_path) catch {};
    defer std.fs.cwd().deleteFile(wal_path) catch {};
    defer std.fs.cwd().deleteFile(shm_path) catch {};

    // ── Writer: create the database and insert the first batch ──────
    var writer_arena = std.heap.ArenaAllocator.init(allocator);
    defer writer_arena.deinit();
    const w_ally = writer_arena.allocator();

    var writer = try Bucket.openFileWithOptions(w_ally, db_path, .{
        .wal = true,
    });
    defer writer.deinit();

    // Insert two documents.
    const doc1 = try bson.fmt.serialize(.{ .seq = @as(i32, 1), .msg = "hello" }, w_ally);
    _ = try writer.insert(doc1);
    const doc2 = try bson.fmt.serialize(.{ .seq = @as(i32, 2), .msg = "world" }, w_ally);
    _ = try writer.insert(doc2);
    try writer.flush();

    // ── Reader: open the same file in WAL mode ──────────────────────
    var reader_arena = std.heap.ArenaAllocator.init(allocator);
    defer reader_arena.deinit();
    const r_ally = reader_arena.allocator();

    var reader = try Bucket.openFileWithOptions(r_ally, db_path, .{
        .wal = true,
    });
    defer reader.deinit();

    // The reader should already see the 2 docs written before it opened.
    var iter_arena = std.heap.ArenaAllocator.init(allocator);
    defer iter_arena.deinit();

    var qDoc = try bson.fmt.serialize(.{ .sector = .{} }, iter_arena.allocator());
    defer qDoc.deinit(iter_arena.allocator());
    const q = try query.Query.parse(iter_arena.allocator(), qDoc);

    var iter = try reader.listIterate(&iter_arena, q);

    var seen: usize = 0;
    while (try iter.next(iter)) |_| {
        seen += 1;
    }
    try testing.expectEqual(@as(usize, 2), seen);

    // ── Writer: insert more documents after the reader exhausted ────
    const doc3 = try bson.fmt.serialize(.{ .seq = @as(i32, 3), .msg = "foo" }, w_ally);
    _ = try writer.insert(doc3);
    const doc4 = try bson.fmt.serialize(.{ .seq = @as(i32, 4), .msg = "bar" }, w_ally);
    _ = try writer.insert(doc4);
    try writer.flush();

    // ── Reader: poll again — should see the 2 new documents ─────────
    // This is the live-tail pattern: the iterator was exhausted, we
    // "sleep" (no-op here), then call next() again which will refresh
    // the header from the WAL SHM and pick up new pages/docs.
    var seen_after: usize = 0;
    while (try iter.next(iter)) |doc| {
        const seq = doc.get("seq").?.int32.value;
        // We should only see seq 3 and 4, not 1 and 2 again.
        try testing.expect(seq == 3 or seq == 4);
        seen_after += 1;
    }
    try testing.expectEqual(@as(usize, 2), seen_after);

    // ── Writer: one more batch to prove repeated polling works ──────
    const doc5 = try bson.fmt.serialize(.{ .seq = @as(i32, 5), .msg = "baz" }, w_ally);
    _ = try writer.insert(doc5);
    try writer.flush();

    var seen_final: usize = 0;
    while (try iter.next(iter)) |doc| {
        const seq = doc.get("seq").?.int32.value;
        try testing.expectEqual(@as(i32, 5), seq);
        seen_final += 1;
    }
    try testing.expectEqual(@as(usize, 1), seen_final);

    // Total: 5 documents via live-tail.
    try testing.expectEqual(@as(usize, 5), seen + seen_after + seen_final);
}

test "WAL live-tail: reader polls empty DB then sees docs after writer inserts" {
    const allocator = std.testing.allocator;
    const db_path = "albedo_test_wal_empty_poll.bucket";
    const wal_path = "albedo_test_wal_empty_poll.bucket-wal";
    const shm_path = "albedo_test_wal_empty_poll.bucket-wal-shm";

    platform.deleteFile(db_path) catch {};
    std.fs.cwd().deleteFile(wal_path) catch {};
    std.fs.cwd().deleteFile(shm_path) catch {};
    defer platform.deleteFile(db_path) catch {};
    defer std.fs.cwd().deleteFile(wal_path) catch {};
    defer std.fs.cwd().deleteFile(shm_path) catch {};

    // ── Reader: open an empty DB and create an iterator ─────────────
    var reader_arena = std.heap.ArenaAllocator.init(allocator);
    defer reader_arena.deinit();
    const r_ally = reader_arena.allocator();

    var reader = try Bucket.openFileWithOptions(r_ally, db_path, .{ .wal = true });
    defer reader.deinit();

    var iter_arena = std.heap.ArenaAllocator.init(allocator);
    defer iter_arena.deinit();

    var qDoc = try bson.fmt.serialize(.{ .sector = .{} }, iter_arena.allocator());
    defer qDoc.deinit(iter_arena.allocator());
    const q = try query.Query.parse(iter_arena.allocator(), qDoc);

    var iter = try reader.listIterate(&iter_arena, q);

    // First poll on empty DB — must return null, not crash.
    const first = try iter.next(iter);
    try testing.expect(first == null);

    // ── Writer: open the same file and insert docs ──────────────────
    var writer_arena = std.heap.ArenaAllocator.init(allocator);
    defer writer_arena.deinit();
    const w_ally = writer_arena.allocator();

    var writer = try Bucket.openFileWithOptions(w_ally, db_path, .{ .wal = true });
    defer writer.deinit();

    const doc1 = try bson.fmt.serialize(.{ .name = "A" }, w_ally);
    _ = try writer.insert(doc1);
    const doc2 = try bson.fmt.serialize(.{ .name = "B" }, w_ally);
    _ = try writer.insert(doc2);
    try writer.flush();

    // ── Reader: poll again — should see both docs ───────────────────
    var seen: usize = 0;
    while (try iter.next(iter)) |_| {
        seen += 1;
    }
    try testing.expectEqual(@as(usize, 2), seen);
}

// ═══════════════════════════════════════════════════════════════════════
// Oplog subscription tests
// ═══════════════════════════════════════════════════════════════════════

test "subscription: reader sees inserts via oplog poll" {
    const allocator = std.testing.allocator;
    const db_path = "albedo_test_sub_insert.bucket";
    const wal_path = "albedo_test_sub_insert.bucket-wal";
    const shm_path = "albedo_test_sub_insert.bucket-wal-shm";

    platform.deleteFile(db_path) catch {};
    std.fs.cwd().deleteFile(wal_path) catch {};
    std.fs.cwd().deleteFile(shm_path) catch {};
    defer platform.deleteFile(db_path) catch {};
    defer std.fs.cwd().deleteFile(wal_path) catch {};
    defer std.fs.cwd().deleteFile(shm_path) catch {};

    // Writer
    var w_arena = std.heap.ArenaAllocator.init(allocator);
    defer w_arena.deinit();
    const w_ally = w_arena.allocator();
    var writer = try Bucket.openFileWithOptions(w_ally, db_path, .{ .wal = true });
    defer writer.deinit();

    // Reader
    var r_arena = std.heap.ArenaAllocator.init(allocator);
    defer r_arena.deinit();
    const r_ally = r_arena.allocator();
    var reader = try Bucket.openFileWithOptions(r_ally, db_path, .{ .wal = true });
    defer reader.deinit();

    // Subscribe with an empty query (match all).
    var qDoc = try bson.fmt.serialize(.{ .sector = .{} }, r_ally);
    defer qDoc.deinit(r_ally);
    const q = try query.Query.parse(r_ally, qDoc);
    var sub = try reader.subscribe(q);
    defer sub.deinit();

    // No events yet.
    const batch0 = try sub.poll(64);
    try testing.expect(batch0 == null);

    // Writer inserts two documents.
    const doc1 = try bson.fmt.serialize(.{ .seq = @as(i32, 1) }, w_ally);
    _ = try writer.insert(doc1);
    const doc2 = try bson.fmt.serialize(.{ .seq = @as(i32, 2) }, w_ally);
    _ = try writer.insert(doc2);
    try writer.flush();

    // Poll events — should return a BSON doc with batch array.
    const batch1 = (try sub.poll(64)) orelse return error.TestExpectedEqual;
    const arr1 = batch1.get("batch") orelse return error.TestExpectedEqual;
    try testing.expectEqual(@as(u32, 2), arr1.array.keyNumber());
    const ev0 = arr1.array.get("0") orelse return error.TestExpectedEqual;
    const ev0_op = ev0.document.get("op") orelse return error.TestExpectedEqual;
    try testing.expectEqualStrings("insert", ev0_op.string.value);

    // Second poll is empty — no new writes.
    const batch2 = try sub.poll(64);
    try testing.expect(batch2 == null);
}

test "subscription: delete events appear in oplog" {
    const allocator = std.testing.allocator;
    const db_path = "albedo_test_sub_delete.bucket";
    const wal_path = "albedo_test_sub_delete.bucket-wal";
    const shm_path = "albedo_test_sub_delete.bucket-wal-shm";

    platform.deleteFile(db_path) catch {};
    std.fs.cwd().deleteFile(wal_path) catch {};
    std.fs.cwd().deleteFile(shm_path) catch {};
    defer platform.deleteFile(db_path) catch {};
    defer std.fs.cwd().deleteFile(wal_path) catch {};
    defer std.fs.cwd().deleteFile(shm_path) catch {};

    var w_arena = std.heap.ArenaAllocator.init(allocator);
    defer w_arena.deinit();
    const w_ally = w_arena.allocator();
    var writer = try Bucket.openFileWithOptions(w_ally, db_path, .{ .wal = true, .auto_vaccuum = false });
    defer writer.deinit();

    // Insert a document before subscribing.
    const doc1 = try bson.fmt.serialize(.{ .tag = "removeme" }, w_ally);
    _ = try writer.insert(doc1);
    try writer.flush();

    // Subscribe after insert.
    var qDoc = try bson.fmt.serialize(.{ .sector = .{} }, w_ally);
    defer qDoc.deinit(w_ally);
    const q = try query.Query.parse(w_ally, qDoc);
    var sub = try writer.subscribe(q);
    defer sub.deinit();

    // Delete the document.
    var delQuery = try bson.fmt.serialize(.{ .query = .{ .tag = "removeme" } }, w_ally);
    defer delQuery.deinit(w_ally);
    const dq = try query.Query.parse(w_ally, delQuery);
    try writer.delete(dq);

    // Poll — should see a delete event.
    const batch = (try sub.poll(64)) orelse return error.TestExpectedEqual;
    const arr = batch.get("batch") orelse return error.TestExpectedEqual;
    try testing.expectEqual(@as(u32, 1), arr.array.keyNumber());
    const ev = arr.array.get("0") orelse return error.TestExpectedEqual;
    const ev_op = ev.document.get("op") orelse return error.TestExpectedEqual;
    try testing.expectEqualStrings("delete", ev_op.string.value);
}

test "subscription: query-filtered subscription skips non-matching inserts" {
    const allocator = std.testing.allocator;
    const db_path = "albedo_test_sub_filter.bucket";
    const wal_path = "albedo_test_sub_filter.bucket-wal";
    const shm_path = "albedo_test_sub_filter.bucket-wal-shm";

    platform.deleteFile(db_path) catch {};
    std.fs.cwd().deleteFile(wal_path) catch {};
    std.fs.cwd().deleteFile(shm_path) catch {};
    defer platform.deleteFile(db_path) catch {};
    defer std.fs.cwd().deleteFile(wal_path) catch {};
    defer std.fs.cwd().deleteFile(shm_path) catch {};

    var w_arena = std.heap.ArenaAllocator.init(allocator);
    defer w_arena.deinit();
    const w_ally = w_arena.allocator();
    var writer = try Bucket.openFileWithOptions(w_ally, db_path, .{ .wal = true });
    defer writer.deinit();

    // Subscribe with filter: only status == "active"
    var qDoc = try bson.fmt.serialize(.{ .query = .{ .status = "active" } }, w_ally);
    defer qDoc.deinit(w_ally);
    const q = try query.Query.parse(w_ally, qDoc);
    var sub = try writer.subscribe(q);
    defer sub.deinit();

    // Insert three docs: two active, one inactive.
    const d1 = try bson.fmt.serialize(.{ .status = "active", .name = "a" }, w_ally);
    _ = try writer.insert(d1);
    const d2 = try bson.fmt.serialize(.{ .status = "inactive", .name = "b" }, w_ally);
    _ = try writer.insert(d2);
    const d3 = try bson.fmt.serialize(.{ .status = "active", .name = "c" }, w_ally);
    _ = try writer.insert(d3);
    try writer.flush();

    // Poll: should see only the two active inserts.
    const batch = (try sub.poll(64)) orelse return error.TestExpectedEqual;
    const arr = batch.get("batch") orelse return error.TestExpectedEqual;
    try testing.expectEqual(@as(u32, 2), arr.array.keyNumber());
    const ev0 = arr.array.get("0") orelse return error.TestExpectedEqual;
    const ev0_op = ev0.document.get("op") orelse return error.TestExpectedEqual;
    try testing.expectEqualStrings("insert", ev0_op.string.value);
}

test "subscription: seqno is monotonically increasing" {
    const allocator = std.testing.allocator;
    const db_path = "albedo_test_sub_seqno.bucket";
    const wal_path = "albedo_test_sub_seqno.bucket-wal";
    const shm_path = "albedo_test_sub_seqno.bucket-wal-shm";

    platform.deleteFile(db_path) catch {};
    std.fs.cwd().deleteFile(wal_path) catch {};
    std.fs.cwd().deleteFile(shm_path) catch {};
    defer platform.deleteFile(db_path) catch {};
    defer std.fs.cwd().deleteFile(wal_path) catch {};
    defer std.fs.cwd().deleteFile(shm_path) catch {};

    var w_arena = std.heap.ArenaAllocator.init(allocator);
    defer w_arena.deinit();
    const w_ally = w_arena.allocator();
    var writer = try Bucket.openFileWithOptions(w_ally, db_path, .{ .wal = true });
    defer writer.deinit();

    var qDoc = try bson.fmt.serialize(.{ .sector = .{} }, w_ally);
    defer qDoc.deinit(w_ally);
    const q = try query.Query.parse(w_ally, qDoc);
    var sub = try writer.subscribe(q);
    defer sub.deinit();

    try testing.expectEqual(@as(u64, 0), sub.currentSeqno());

    // Insert 5 documents.
    var i: i32 = 0;
    while (i < 5) : (i += 1) {
        const doc = try bson.fmt.serialize(.{ .i = i }, w_ally);
        _ = try writer.insert(doc);
    }
    try writer.flush();

    try testing.expectEqual(@as(u64, 5), sub.currentSeqno());

    // Poll all and verify monotonicity.
    const batch_doc = (try sub.poll(64)) orelse return error.TestExpectedEqual;
    const arr = batch_doc.get("batch") orelse return error.TestExpectedEqual;
    try testing.expectEqual(@as(u32, 5), arr.array.keyNumber());
    var prev_seqno: i64 = 0;
    for (0..5) |idx| {
        var key_buf: [20]u8 = undefined;
        const key = std.fmt.bufPrint(&key_buf, "{d}", .{idx}) catch unreachable;
        const ev = arr.array.get(key) orelse return error.TestExpectedEqual;
        const seqno = ev.document.get("seqno") orelse return error.TestExpectedEqual;
        try testing.expect(seqno.int64.value > prev_seqno);
        prev_seqno = seqno.int64.value;
    }
}

test "$or index union: uses index scan when all branches are covered" {
    const allocator = testing.allocator;

    var bucket = try Bucket.init(allocator, ":memory:");
    defer bucket.deinit();

    // Create indexes on "role" and "public".
    try bucket.ensureIndex("role", .{});
    try bucket.ensureIndex("public", .{});

    // Insert four documents.
    const doc_admin = try bson.fmt.serialize(.{ .role = "admin", .public = false }, allocator);
    defer doc_admin.deinit(allocator);
    _ = try bucket.insert(doc_admin);

    const doc_public = try bson.fmt.serialize(.{ .role = "user", .public = true }, allocator);
    defer doc_public.deinit(allocator);
    _ = try bucket.insert(doc_public);

    const doc_neither = try bson.fmt.serialize(.{ .role = "user", .public = false }, allocator);
    defer doc_neither.deinit(allocator);
    _ = try bucket.insert(doc_neither);

    const doc_both = try bson.fmt.serialize(.{ .role = "admin", .public = true }, allocator);
    defer doc_both.deinit(allocator);
    _ = try bucket.insert(doc_both);

    // Query: { "$or": [{"role": "admin"}, {"public": true}] }
    // Should match doc_admin, doc_public, and doc_both (3 documents).
    const filter_doc = try bson.fmt.serialize(.{
        .query = .{
            .@"$or" = .{ .{ .role = "admin" }, .{ .public = true } },
        },
    }, allocator);
    defer filter_doc.deinit(allocator);

    var q = try query.Query.parse(allocator, filter_doc);
    defer q.deinit(allocator);

    // Use an arena for list results; doc.buffer slices point into the page cache
    // and must not be individually freed.
    var result_arena = std.heap.ArenaAllocator.init(allocator);
    defer result_arena.deinit();

    const results = try bucket.list(result_arena.allocator(), q);
    // Three documents match: admin-not-public, user-public, admin-public.
    try testing.expectEqual(@as(usize, 3), results.len);
}
