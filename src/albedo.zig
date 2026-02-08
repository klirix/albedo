const std = @import("std");
const bson = @import("bson.zig");
const BSONValue = bson.BSONValue;
const BSONDocument = bson.BSONDocument;
const mem = std.mem;
const platform = @import("platform.zig");
const ObjectId = @import("object_id.zig").ObjectId;
const ObjectIdGenerator = @import("object_id.zig").ObjectIdGenerator;
const query = @import("query.zig");
const bindex = @import("bplusindex.zig");
const Index = bindex.Index;
const IndexOptions = bindex.IndexOptions;
const encryption = @import("encryption.zig");

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
    encryption_salt: [16]u8, // Salt for key derivation (when ENCRYPTED_FLAG is set)
    reserved: [12]u8, // Reduced padding after adding encryption_salt

    const byteSize = 64;
    const ENCRYPTED_FLAG: u8 = 0x01;

    pub fn init() BucketHeader {
        var header = BucketHeader{
            .magic = undefined,
            .version = ALBEDO_VERSION,
            .flags = ALBEDO_FLAGS,
            .page_size = DEFAULT_PAGE_SIZE,
            .page_count = 0,
            .doc_count = 0,
            .deleted_count = 0,
            .encryption_salt = [_]u8{0} ** 16,
            .reserved = [_]u8{0} ** 12,
        };

        std.mem.copyForwards(u8, &header.magic, ALBEDO_MAGIC);

        return header;
    }

    pub fn isEncrypted(self: *const BucketHeader) bool {
        return (self.flags & ENCRYPTED_FLAG) != 0;
    }


    pub fn setEncrypted(self: *BucketHeader, salt: [16]u8) void {
        self.flags |= ENCRYPTED_FLAG;
        self.encryption_salt = salt;
    }

    // Note: encryption is always strict when enabled.

    pub fn read(memory: []const u8) BucketHeader {
        var header = BucketHeader{
            .magic = undefined,
            .version = memory[6],
            .flags = memory[7],
            .page_size = std.mem.readInt(u32, memory[8..12], .little),
            .page_count = std.mem.readInt(u64, memory[12..20], .little),
            .doc_count = std.mem.readInt(u64, memory[20..28], .little),
            .deleted_count = std.mem.readInt(u64, memory[28..36], .little),
            .encryption_salt = undefined,
            .reserved = undefined,
        };

        std.mem.copyForwards(u8, header.magic[0..], ALBEDO_MAGIC);
        std.mem.copyForwards(u8, &header.encryption_salt, memory[36..52]);
        std.mem.copyForwards(u8, &header.reserved, memory[52..64]);

        return header;
    }

    pub fn toBytes(self: *const BucketHeader) [byteSize]u8 {
        var buffer: [byteSize]u8 = undefined;
        std.mem.copyForwards(u8, buffer[0..6], ALBEDO_MAGIC);
        buffer[6] = self.version;
        buffer[7] = self.flags;
        std.mem.writeInt(u32, buffer[8..12], self.page_size, .little);
        std.mem.writeInt(u64, buffer[12..20], self.page_count, .little);
        std.mem.writeInt(u64, buffer[20..28], self.doc_count, .little);
        std.mem.writeInt(u64, buffer[28..36], self.deleted_count, .little);
        std.mem.copyForwards(u8, buffer[36..52], &self.encryption_salt);
        std.mem.copyForwards(u8, buffer[52..64], &self.reserved);
        return buffer;
    }

    pub fn write(self: *const BucketHeader, writer: *std.Io.Writer) !void {
        const bytes = self.toBytes();
        _ = try writer.write(&bytes);
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
    // Encryption metadata (19 bytes total)
    encryption_flags: u8 = 0, // 1 - encryption status and cipher type
    nonce: [12]u8 = [_]u8{0} ** 12, // 12 - nonce for ChaCha20-Poly1305
    key_version: u8 = 0, // 1 - for key rotation support
    reserved: [5]u8 = [_]u8{0} ** 5, // 5 - future use

    pub const byteSize = 32;

    pub fn init(page_type: PageType, id: u64) PageHeader {
        return PageHeader{
            .page_type = page_type,
            .used_size = 0,
            .page_id = id,
            .first_readable_byte = 0,
            .encryption_flags = 0,
            .nonce = [_]u8{0} ** 12,
            .key_version = 0,
            .reserved = [_]u8{0} ** 5,
        };
    }

    pub fn isEncrypted(self: *const PageHeader) bool {
        const flags = encryption.EncryptionFlags.fromByte(self.encryption_flags);
        return flags.isEncrypted();
    }

    pub fn getCipherType(self: *const PageHeader) encryption.CipherType {
        const flags = encryption.EncryptionFlags.fromByte(self.encryption_flags);
        return flags.getCipherType();
    }

    pub fn setEncryption(self: *PageHeader, cipher: encryption.CipherType, nonce: [12]u8, key_version: u8) void {
        var flags = encryption.EncryptionFlags{};
        flags.setCipher(cipher);
        self.encryption_flags = flags.toByte();
        self.nonce = nonce;
        self.key_version = key_version;
    }

    pub fn write(self: *const PageHeader) [32]u8 {
        var buffer: [32]u8 = [_]u8{0} ** 32;
        buffer[0] = @intFromEnum(self.page_type);
        std.mem.writeInt(u16, buffer[1..3], self.used_size, .little);
        std.mem.writeInt(u64, buffer[3..11], self.page_id, .little);
        std.mem.writeInt(u16, buffer[11..13], self.first_readable_byte, .little);
        buffer[13] = self.encryption_flags;
        std.mem.copyForwards(u8, buffer[14..26], &self.nonce);
        buffer[26] = self.key_version;
        std.mem.copyForwards(u8, buffer[27..32], &self.reserved);
        return buffer;
    }

    pub fn read(memory: []const u8) PageHeader {
        var nonce: [12]u8 = undefined;
        std.mem.copyForwards(u8, &nonce, memory[14..26]);

        var reserved: [5]u8 = undefined;
        std.mem.copyForwards(u8, &reserved, memory[27..32]);

        const header = PageHeader{
            .page_type = @enumFromInt(std.mem.readInt(u8, memory[0..1], .little)),
            .used_size = std.mem.readInt(u16, memory[1..3], .little),
            .page_id = std.mem.readInt(u64, memory[3..11], .little),
            .first_readable_byte = std.mem.readInt(u16, memory[11..13], .little),
            .encryption_flags = memory[13],
            .nonce = nonce,
            .key_version = memory[26],
            .reserved = reserved,
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
    MissingEncryptionKey,
    EncryptionAuthenticationFailed,
    DecryptionFailed,
    PlaintextPageInEncryptedBucket,
    KeyVersionNotFound,
};

/// Callback function type for page replication
/// Called after pages are written and synced to disk
/// The data buffer contains a batch: [BucketHeader (64 bytes)][Page1 (8192 bytes)][Page2 (8192 bytes)]...
/// Page IDs can be extracted by reading the page headers within the buffer
/// Each page in the buffer is at offset: 64 + (page_index * 8192)
/// Returns 0 on success, non-zero error code on failure (will retry on next sync)
pub const PageChangeCallback = ?*const fn (
    context: ?*anyopaque, // User-provided context
    data: [*]const u8, // Raw data: header (64 bytes) + N pages (8192 bytes each)
    data_size: u32, // Total size of data (BucketHeader.byteSize + page_count * DEFAULT_PAGE_SIZE)
    page_count: u32, // Number of pages in the batch
) callconv(.c) u8;

/// Error codes returned by replication callback
pub const ReplicationError = enum(u8) {
    OK = 0,
    NetworkError = 1,
    DiskFull = 2,
    InvalidFormat = 3,
    ReplicaUnavailable = 4,
    TimeoutError = 5,
    UnknownError = 255,
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
    sync_threshold: u32 = 100, // Sync after every N writes
    replication_callback: PageChangeCallback = null,
    replication_context: ?*anyopaque = null,
    dirty_pages: std.AutoHashMap(u64, void),
    replication_retry_count: u32 = 0,
    max_replication_retries: u32 = 3,
    // Encryption support
    encryption_key: ?[encryption.KEY_SIZE]u8 = null,
    encryption_key_version: u8 = 0,
    encryption_keys: std.AutoHashMap(u8, [encryption.KEY_SIZE]u8),

    pub const EncryptionKeyEntry = struct {
        version: u8,
        key: [encryption.KEY_SIZE]u8,
    };

    const KeyringConfig = struct {
        keys: []const EncryptionKeyEntry,
        active_version: u8,
    };

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
                        return null;
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

    pub fn initWithEncryptionKeys(
        allocator: std.mem.Allocator,
        path: []const u8,
        keys: []const EncryptionKeyEntry,
        active_version: u8,
    ) BucketInitErrors!Bucket {
        return Bucket.openFileWithEncryptionKeys(allocator, path, keys, active_version);
    }

    const BucketFileMode = enum {
        ReadOnly,
        ReadWrite,
    };

    const OpenBucketOptions = struct {
        buildIdIndex: bool = false,
        mode: BucketFileMode = BucketFileMode.ReadWrite,
        autoVaccuum: bool = true,
        page_cache_capacity: usize = DEFAULT_PAGE_CACHE_CAPACITY,
    };

    fn getEncryptionKeyForVersion(self: *const Bucket, version: u8) ?[encryption.KEY_SIZE]u8 {
        if (self.encryption_key) |key| {
            if (self.encryption_key_version == version) return key;
        }
        if (self.encryption_keys.get(version)) |k| return k;
        return null;
    }

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

            const idx = Index.loadWithOptions(self, page_id, options) catch |err| {
                // Preserve the underlying error (e.g. encryption auth failure) so open() can map it.
                return err;
            };

            // Insert the index into the hash map
            try self.indexes.put(key, idx);
        }
    }

    fn recordIndexes(self: *Bucket) !void {
        const meta_page = try self.loadPage(0);

        var buffer: [DEFAULT_PAGE_SIZE - @sizeOf(PageHeader)]u8 = undefined;
        // If encryption is enabled, we need room for the auth tag on disk.
        const plaintext_limit: usize = if (self.encryption_key != null)
            (buffer.len - encryption.AUTH_TAG_SIZE)
        else
            buffer.len;

        var writer = std.io.Writer.fixed(buffer[0..plaintext_limit]);

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
        // Zero the remainder (including the auth-tag reserved region when encryption is enabled)
        @memset(buffer[plaintext_limit..], 0);
        @memcpy(meta_page.data, &buffer);

        // Persist full plaintext region so encryption and non-encryption both work.
        meta_page.header.used_size = @intCast(plaintext_limit);

        try self.writePage(meta_page);
    }

    pub fn ensureIndex(self: *Bucket, path: []const u8, options: IndexOptions) !void {
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

    pub fn dropIndex(self: *Bucket, path: []const u8) !void {
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

    fn createEmptyDBFile(
        path: []const u8,
        ally: mem.Allocator,
        options: OpenBucketOptions,
        passphrase: ?[]const u8,
        keyring: ?KeyringConfig,
    ) BucketInitErrors!Bucket {
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

        var bucket = Bucket{
            .file = new_file,
            .path = stored_path,
            .header = .init(),
            .allocator = ally,
            .pageCache = PageCache.init(ally, options.page_cache_capacity),
            .indexes = .init(ally),
            .autoVaccuum = options.autoVaccuum,
            .objectIdGenerator = generator,
            .dirty_pages = std.AutoHashMap(u64, void).init(ally),
            .encryption_keys = std.AutoHashMap(u8, [encryption.KEY_SIZE]u8).init(ally),
        };

        // If encryption is requested at creation time, apply it BEFORE writing meta/index pages.
        if (keyring) |cfg| {
            bucket.setEncryptionKeyring(cfg.keys, cfg.active_version) catch |err| {
                return switch (err) {
                    error.OutOfMemory => BucketInitErrors.OutOfMemory,
                    BucketInitErrors.KeyVersionNotFound => BucketInitErrors.InitializationError,
                    else => BucketInitErrors.FileWriteError,
                };
            };
        } else if (passphrase) |pp| {
            bucket.setEncryptionPassphrase(pp) catch |err| {
                return switch (err) {
                    error.OutOfMemory => BucketInitErrors.OutOfMemory,
                    else => BucketInitErrors.FileWriteError,
                };
            };
        }

        bucket.flushHeader() catch return BucketInitErrors.FileWriteError;

        const meta = bucket.createNewPage(.Meta) catch return BucketInitErrors.InitializationError;
        // Write the initial empty meta page
        bucket.writePage(meta) catch return BucketInitErrors.FileWriteError;

        // Now ensure the _id index - this will call recordIndexes() which writes to the meta page
        bucket.ensureIndex("_id", .{ .unique = 1 }) catch return BucketInitErrors.InitializationError;

        bucket.rwlock = .{};

        return bucket;
    }

    fn createInMemoryBucket(
        ally: mem.Allocator,
        options: OpenBucketOptions,
        passphrase: ?[]const u8,
        keyring: ?KeyringConfig,
    ) BucketInitErrors!Bucket {
        const generator = ObjectIdGenerator.init() catch return BucketInitErrors.UnexpectedError;
        var bucket = Bucket{
            .file = null,
            .path = ally.dupe(u8, ":memory:") catch return BucketInitErrors.OutOfMemory,
            .header = .init(),
            .allocator = ally,
            .pageCache = PageCache.init(ally, std.math.maxInt(usize)),
            .indexes = .init(ally),
            .autoVaccuum = options.autoVaccuum,
            .objectIdGenerator = generator,
            .in_memory = true,
            .dirty_pages = std.AutoHashMap(u64, void).init(ally),
            .encryption_keys = std.AutoHashMap(u8, [encryption.KEY_SIZE]u8).init(ally),
        };

        if (keyring) |cfg| {
            bucket.setEncryptionKeyring(cfg.keys, cfg.active_version) catch |err| {
                return switch (err) {
                    error.OutOfMemory => BucketInitErrors.OutOfMemory,
                    BucketInitErrors.KeyVersionNotFound => BucketInitErrors.InitializationError,
                    else => BucketInitErrors.InitializationError,
                };
            };
        } else if (passphrase) |pp| {
            bucket.setEncryptionPassphrase(pp) catch |err| {
                return switch (err) {
                    error.OutOfMemory => BucketInitErrors.OutOfMemory,
                    else => BucketInitErrors.InitializationError,
                };
            };
        }

        const meta = bucket.createNewPage(.Meta) catch return BucketInitErrors.InitializationError;
        // Write the initial empty meta page
        bucket.writePage(meta) catch return BucketInitErrors.FileWriteError;

        // Now ensure the _id index - this will call recordIndexes() which writes to the meta page
        bucket.ensureIndex("_id", .{ .unique = 1 }) catch return BucketInitErrors.InitializationError;

        bucket.rwlock = .{};

        return bucket;
    }

    // Path may be relative or absolute
    pub fn openFileWithOptions(ally: std.mem.Allocator, path: []const u8, options: OpenBucketOptions) BucketInitErrors!Bucket {
        return Bucket.openFileWithOptionsAndPassphrase(ally, path, null, options);
    }

    pub fn openFileWithPassphrase(ally: std.mem.Allocator, path: []const u8, passphrase: []const u8) BucketInitErrors!Bucket {
        return Bucket.openFileWithOptionsAndPassphrase(ally, path, passphrase, .{});
    }

    pub fn initWithPassphrase(allocator: std.mem.Allocator, path: []const u8, passphrase: []const u8) BucketInitErrors!Bucket {
        return Bucket.openFileWithOptionsAndPassphrase(allocator, path, passphrase, .{});
    }

    /// Open a bucket with an optional passphrase.
    /// If the bucket file header indicates encryption, the passphrase is required
    /// before loading the meta page so we can decrypt index metadata.
    pub fn openFileWithOptionsAndPassphrase(
        ally: std.mem.Allocator,
        path: []const u8,
        passphrase: ?[]const u8,
        options: OpenBucketOptions,
    ) BucketInitErrors!Bucket {
        if (mem.eql(u8, path, ":memory:")) {
            // In-memory buckets do not have on-disk pages; encryption is currently a file-backed feature.
            return createInMemoryBucket(ally, options, null, null);
        }

        var file = platform.openFile(ally, path, .{
            .read = true,
            .write = options.mode == .ReadWrite,
        }) catch |err| switch (err) {
            error.FileNotFound => {
                if (options.mode != .ReadWrite) return BucketInitErrors.FileNotFound;
                return createEmptyDBFile(path, ally, options, passphrase, null);
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

        var bucket = Bucket{
            .file = file,
            .path = stored_path,
            .allocator = ally,
            .header = .read(header_bytes[0..BucketHeader.byteSize]),
            .pageCache = PageCache.init(ally, options.page_cache_capacity),
            .indexes = .init(ally),
            .autoVaccuum = options.autoVaccuum,
            .objectIdGenerator = generator,
            .dirty_pages = std.AutoHashMap(u64, void).init(ally),
            .encryption_keys = std.AutoHashMap(u8, [encryption.KEY_SIZE]u8).init(ally),
        };
        errdefer {
            if (bucket.file) |*fh| fh.close();
            bucket.pageCache.deinit();
            bucket.indexes.deinit();
            ally.free(bucket.path);
        }

        // Encryption is strict whenever enabled.

        // If a passphrase is provided, derive/set the key before reading the meta page.
        // This allows encrypted meta/index pages to be decrypted during open.
        if (passphrase) |pp| {
            if (bucket.header.isEncrypted()) {
                bucket.setEncryptionPassphrase(pp) catch return BucketInitErrors.UnexpectedError;
            }
        }

        const meta = bucket.loadPage(0) catch |err| {
            return switch (err) {
                error.MissingEncryptionKey => BucketInitErrors.MissingEncryptionKey,
                error.EncryptionAuthenticationFailed => BucketInitErrors.EncryptionAuthenticationFailed,
                error.DecryptionFailed => BucketInitErrors.DecryptionFailed,
                BucketInitErrors.PlaintextPageInEncryptedBucket => BucketInitErrors.PlaintextPageInEncryptedBucket,
                else => BucketInitErrors.FileReadError,
            };
        };

        bucket.loadIndices(meta) catch |err| {
            return switch (err) {
                error.MissingEncryptionKey => BucketInitErrors.MissingEncryptionKey,
                error.EncryptionAuthenticationFailed => BucketInitErrors.EncryptionAuthenticationFailed,
                error.DecryptionFailed => BucketInitErrors.DecryptionFailed,
                else => BucketInitErrors.LoadIndexError,
            };
        };

        return bucket;
    }

    pub fn openFileWithEncryptionKeys(
        ally: std.mem.Allocator,
        path: []const u8,
        keys: []const EncryptionKeyEntry,
        active_version: u8,
    ) BucketInitErrors!Bucket {
        return Bucket.openFileWithOptionsAndEncryptionKeys(ally, path, keys, active_version, .{});
    }

    pub fn openFileWithOptionsAndEncryptionKeys(
        ally: std.mem.Allocator,
        path: []const u8,
        keys: []const EncryptionKeyEntry,
        active_version: u8,
        options: OpenBucketOptions,
    ) BucketInitErrors!Bucket {
        const keyring: KeyringConfig = .{ .keys = keys, .active_version = active_version };

        if (mem.eql(u8, path, ":memory:")) {
            return createInMemoryBucket(ally, options, null, null);
        }

        var file = platform.openFile(ally, path, .{
            .read = true,
            .write = options.mode == .ReadWrite,
        }) catch |err| switch (err) {
            error.FileNotFound => {
                if (options.mode != .ReadWrite) return BucketInitErrors.FileNotFound;
                return createEmptyDBFile(path, ally, options, null, keyring);
            },
            else => return BucketInitErrors.FileOpenError,
        };

        var header_bytes: [BucketHeader.byteSize]u8 = undefined;
        file.preadAll(header_bytes[0..], 0) catch {
            file.close();
            return BucketInitErrors.FileReadError;
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

        var bucket = Bucket{
            .file = file,
            .path = stored_path,
            .allocator = ally,
            .header = .read(header_bytes[0..BucketHeader.byteSize]),
            .pageCache = PageCache.init(ally, options.page_cache_capacity),
            .indexes = .init(ally),
            .autoVaccuum = options.autoVaccuum,
            .objectIdGenerator = generator,
            .dirty_pages = std.AutoHashMap(u64, void).init(ally),
            .encryption_keys = std.AutoHashMap(u8, [encryption.KEY_SIZE]u8).init(ally),
        };
        errdefer {
            if (bucket.file) |*fh| fh.close();
            bucket.pageCache.deinit();
            bucket.indexes.deinit();
            bucket.encryption_keys.deinit();
            ally.free(bucket.path);
        }

        // Encryption is strict whenever enabled.

        if (bucket.header.isEncrypted()) {
            // Install keys before reading meta/index pages.
            bucket.setEncryptionKeyring(keys, active_version) catch return BucketInitErrors.UnexpectedError;
        }

        const meta = bucket.loadPage(0) catch |err| {
            return switch (err) {
                error.MissingEncryptionKey => BucketInitErrors.MissingEncryptionKey,
                error.EncryptionAuthenticationFailed => BucketInitErrors.EncryptionAuthenticationFailed,
                error.DecryptionFailed => BucketInitErrors.DecryptionFailed,
                BucketInitErrors.PlaintextPageInEncryptedBucket => BucketInitErrors.PlaintextPageInEncryptedBucket,
                else => BucketInitErrors.FileReadError,
            };
        };

        bucket.loadIndices(meta) catch |err| {
            return switch (err) {
                error.MissingEncryptionKey => BucketInitErrors.MissingEncryptionKey,
                error.EncryptionAuthenticationFailed => BucketInitErrors.EncryptionAuthenticationFailed,
                error.DecryptionFailed => BucketInitErrors.DecryptionFailed,
                else => BucketInitErrors.LoadIndexError,
            };
        };

        return bucket;
    }

    fn flushHeader(self: *Bucket) !void {
        if (self.in_memory) {
            return;
        }

        const file = if (self.file) |*f| f else return error.StorageUnavailable;
        const bytes = self.header.toBytes();
        try file.pwriteAll(bytes[0..], 0);
        try file.sync();
    }

    pub fn buildIndex(self: *Bucket, path: []const u8, options: IndexOptions) !void {
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

            defer temp_allocator.free(doc_result.data);

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

        final_index_owner = false;
        key_owner = false;
        inserted_into_map = false;
    }

    // Previous openFile implementation remains the same...

    pub fn loadPage(self: *Bucket, page_id: u64) !*Page {
        // Check if the page is already in the cache
        if (self.pageCache.get(page_id)) |page| {
            return page;
        }

        if (self.in_memory) {
            return PageError.PageNotFound;
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

        if (self.header.isEncrypted() and !header.isEncrypted()) {
            return BucketInitErrors.PlaintextPageInEncryptedBucket;
        }

        const page = try self.allocator.create(Page);
        page.* = try Page.init(self.allocator, self, header);

        if (page.header.page_id != page_id) {
            Page.deinit(page, self.allocator);
            self.allocator.destroy(page);
            return PageError.InvalidPageId;
        }

        // Read page data from disk
        var disk_data: [DEFAULT_PAGE_SIZE - PageHeader.byteSize]u8 = undefined;
        file.preadAll(&disk_data, offset + PageHeader.byteSize) catch |err| {
            Page.deinit(page, self.allocator);
            self.allocator.destroy(page);
            return switch (err) {
                error.FileNotFound => PageError.PageNotFound,
                else => PageError.PageNotFound,
            };
        };

        // Decrypt if page is encrypted
        if (header.isEncrypted()) {
            const key = self.getEncryptionKeyForVersion(header.key_version) orelse {
                Page.deinit(page, self.allocator);
                self.allocator.destroy(page);
                return error.MissingEncryptionKey;
            };

            const decrypted_size = encryption.decryptPage(
                disk_data[0..header.used_size],
                page.data,
                key,
                header.nonce,
                page_id,
            ) catch |err| {
                Page.deinit(page, self.allocator);
                self.allocator.destroy(page);
                return switch (err) {
                    error.AuthenticationFailed => error.EncryptionAuthenticationFailed,
                    else => error.DecryptionFailed,
                };
            };

            // Update used_size to reflect decrypted size (without auth tag)
            page.header.used_size = @intCast(decrypted_size);
        } else {
            // Not encrypted, just copy data
            @memcpy(page.data, &disk_data);
        }

        if (try self.pageCache.put(page_id, page)) |evicted| {
            // Don't destroy evicted pages immediately - they may still be referenced
            // by B+ tree Node structures. They will be freed during pageCache.clear()
            _ = evicted;
        }

        return page;
    }

    pub fn writePage(self: *Bucket, page: *const Page) !void {
        if (self.in_memory) {
            return;
        }

        const file = if (self.file) |*f| f else return error.StorageUnavailable;

        const offset = BucketHeader.byteSize + (page.header.page_id * DEFAULT_PAGE_SIZE);

        // If the bucket is marked encrypted, never allow plaintext writes without a key.
        if (self.header.isEncrypted() and self.encryption_key == null) {
            return error.MissingEncryptionKey;
        }

        // Encrypt if encryption is enabled
        if (self.encryption_key) |key| {
            // Prepare header and data for writing
            var write_header = page.header;
            var disk_data: [DEFAULT_PAGE_SIZE - PageHeader.byteSize]u8 = undefined;
            @memset(&disk_data, 0);

            // Ensure we have space for ciphertext + tag within one page.
            if (page.header.used_size > disk_data.len - encryption.AUTH_TAG_SIZE) {
                return error.PageTooFullForEncryption;
            }

            // Generate nonce for this write
            const nonce = encryption.generateNonce();

            // Encrypt the page data
            const plaintext = page.data[0..page.header.used_size];
            const encrypted_size = try encryption.encryptPage(
                plaintext,
                &disk_data,
                key,
                nonce,
                page.header.page_id,
            );

            // Update header with encryption metadata
            write_header.setEncryption(.chacha20_poly1305, nonce, self.encryption_key_version);
            write_header.used_size = @intCast(encrypted_size);

            const header_bytes = PageHeader.write(&write_header);
            try file.pwriteAll(header_bytes[0..], offset);
            try file.pwriteAll(&disk_data, offset + PageHeader.byteSize);
        } else {
            // Not encrypting: write the full page payload (preserves meta/index contents even if used_size isn't tracked).
            const header_bytes = PageHeader.write(&page.header);
            try file.pwriteAll(header_bytes[0..], offset);
            try file.pwriteAll(page.data, offset + PageHeader.byteSize);
        }

        // Track dirty page for replication (set automatically handles duplicates)
        if (self.replication_callback != null) {
            try self.dirty_pages.put(page.header.page_id, {});
        }

        // Batched sync: only sync periodically instead of every write
        self.writes_since_sync += 1;
        if (self.writes_since_sync >= self.sync_threshold) {
            try file.sync();
            self.writes_since_sync = 0;

            // Trigger replication callback after sync (non-fatal)
            self.notifyDirtyPages() catch {
                // Replication error - will retry on next sync
            };
        }
    }

    /// Notify replication callback of dirty pages and clear the list
    fn notifyDirtyPages(self: *Bucket) !void {
        if (self.replication_callback == null or self.dirty_pages.count() == 0) {
            return;
        }

        const callback = self.replication_callback.?;
        const page_count = self.dirty_pages.count();

        // Calculate total buffer size: header + all dirty pages
        const total_size = BucketHeader.byteSize + (page_count * DEFAULT_PAGE_SIZE);

        // Allocate buffer for batched replication
        const buffer = try self.allocator.alloc(u8, total_size);
        defer self.allocator.free(buffer);

        // First 64 bytes: bucket header
        const header_bytes = self.header.toBytes();
        @memcpy(buffer[0..BucketHeader.byteSize], &header_bytes);

        // Pack all dirty pages into buffer
        var iterator = self.dirty_pages.keyIterator();
        var i: usize = 0;
        while (iterator.next()) |page_id_ptr| : (i += 1) {
            const page = try self.loadPage(page_id_ptr.*);
            const offset = BucketHeader.byteSize + (i * DEFAULT_PAGE_SIZE);

            // Write page header (32 bytes)
            const page_header_bytes = PageHeader.write(&page.header);
            @memcpy(buffer[offset .. offset + PageHeader.byteSize], &page_header_bytes);

            // Write page data
            @memcpy(buffer[offset + PageHeader.byteSize .. offset + DEFAULT_PAGE_SIZE], page.data);
        }

        // Call the callback once with all pages and check acknowledgement
        const result = callback(
            self.replication_context,
            buffer.ptr,
            @intCast(total_size),
            @intCast(page_count),
        );

        // Handle callback result
        if (result == @intFromEnum(ReplicationError.OK)) {
            // Success: clear dirty pages and reset retry counter
            self.dirty_pages.clearRetainingCapacity();
            self.replication_retry_count = 0;
        } else {
            // Failure: keep dirty pages for retry
            self.replication_retry_count += 1;

            // Check if we've exceeded max retries
            if (self.replication_retry_count >= self.max_replication_retries) {
                // Log or handle max retries exceeded
                // For now, clear pages to prevent infinite retry
                // (application can implement custom logic via error codes)
                self.dirty_pages.clearRetainingCapacity();
                self.replication_retry_count = 0;
                return error.ReplicationMaxRetriesExceeded;
            }

            return error.ReplicationFailed;
        }
    }

    /// Force a sync of all pending writes to disk.
    /// Call this when you need guaranteed durability (e.g., after critical operations).
    pub fn flush(self: *Bucket) !void {
        if (self.in_memory) {
            return;
        }

        if (self.file) |*f| {
            try f.sync();
        }

        self.writes_since_sync = 0;

        // Trigger replication callback after flush
        // Note: Replication errors are non-fatal - dirty pages kept for retry
        self.notifyDirtyPages() catch {
            // Replication failed, but data is safely on disk
            // Dirty pages will be retried on next sync
        };
    }

    /// Apply replicated pages to this bucket (for replicas)
    /// This writes raw page data directly to disk and invalidates the cache
    /// Buffer format: [BucketHeader (64 bytes)][Page1 (8192)][Page2 (8192)]...
    pub fn applyReplicatedBatch(self: *Bucket, data: []const u8, page_count: u32) !void {
        if (self.in_memory) {
            return error.ReplicationNotSupported;
        }

        const expected_size = BucketHeader.byteSize + (@as(usize, page_count) * DEFAULT_PAGE_SIZE);
        if (data.len != expected_size) {
            return error.InvalidBatchSize;
        }

        const file = if (self.file) |*f| f else return error.StorageUnavailable;

        // Extract and apply bucket header (first 64 bytes)
        const header_data = data[0..BucketHeader.byteSize];
        const new_header = BucketHeader.read(header_data);

        // Update our header with the replicated values
        self.header = new_header;
        try self.flushHeader();

        var meta_page_replicated = false;

        // Process each page in the batch
        var i: u32 = 0;
        while (i < page_count) : (i += 1) {
            const page_data_start = BucketHeader.byteSize + (@as(usize, i) * DEFAULT_PAGE_SIZE);
            const page_data_end = page_data_start + DEFAULT_PAGE_SIZE;
            const page_data = data[page_data_start..page_data_end];

            // Parse page header to get page_id
            const page_header = PageHeader.read(page_data[0..PageHeader.byteSize]);
            const page_id = page_header.page_id;

            // Calculate file offset for this page
            const page_offset = BucketHeader.byteSize + (page_id * DEFAULT_PAGE_SIZE);

            // Write page data directly to file
            try file.pwriteAll(page_data, page_offset);

            // Invalidate page cache entry if it exists
            if (self.pageCache.remove(page_id)) |old_page| {
                old_page.deinit(self.allocator);
                self.allocator.destroy(old_page);
            }

            // Track if meta page was replicated
            if (page_id == 0) {
                meta_page_replicated = true;
            }
        }

        // Sync all changes to disk
        try file.sync();

        // Special handling for page 0 (meta page): reload indexes
        if (meta_page_replicated) {
            // Clear existing indexes
            var idx_iter = self.indexes.iterator();
            while (idx_iter.next()) |pair| {
                const index_ptr = pair.value_ptr.*;
                const key = pair.key_ptr.*;
                self.allocator.destroy(index_ptr);
                self.allocator.free(key);
            }
            self.indexes.clearRetainingCapacity();

            // Reload indexes from the newly written meta page
            const meta_page = try self.loadPage(0);
            try self.loadIndices(meta_page);
        }
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

        // Find the last data page
        var pageIter = PageIterator{
            .bucket = self,
            .index = self.header.page_count,
            .type = PageType.Data,
            .reverse = true,
        };
        while (try pageIter.next()) |page| {
            if (page.header.page_type == PageType.Data) {
                return page;
            }
        }
        return null;
    }

    pub const InsertError = error{
        DuplicateKey,
    };

    pub fn insert(self: *Bucket, insertable: bson.BSONDocument) !DocInsertResult {
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

        self.rwlock.lock();
        defer {
            self.rwlock.unlock();
        }

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
            page = p;
        } else {
            page = try self.createNewPage(.Data);
            try self.writePage(page);
        }

        const page_plaintext_limit: usize = if (self.encryption_key != null)
            (page.data.len - encryption.AUTH_TAG_SIZE)
        else
            page.data.len;

        // Check if the page has enough space for header and doc size
        if (page.header.used_size >= page_plaintext_limit or (page_plaintext_limit - page.header.used_size) <= (4 + DocHeader.byteSize)) { // 20 bytes
            // Not enough space, create a new page
            page = try self.createNewPage(.Data);
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
            const effective_limit: usize = if (self.encryption_key != null)
                (page.data.len - encryption.AUTH_TAG_SIZE)
            else
                page.data.len;
            const available_space = if (offset.* >= effective_limit) 0 else (effective_limit - offset.*);
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
                plan.index.insert(value, index_location) catch |err| switch (err) {
                    error.DuplicateKey => return error.DuplicateKey,
                    else => return err,
                };
            }
        }

        self.header.doc_count += 1;
        try self.flushHeader();

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
                self.page = try self.pageIterator.next() orelse return null;
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
        };

        const IndexBounds = struct {
            lower: ?Index.RangeBound = null,
            upper: ?Index.RangeBound = null,
        };

        source: Source = .full_scan,
        index: ?*Index = null,
        filter_index: ?usize = null,
        index_path: ?[]const u8 = null,
        bounds: IndexBounds = .{},
        eager: bool = false,
        sort_covered: bool = false,
        index_strategy: IndexStrategy = .range,
    };

    inline fn planUsesPointStrategy(plan: *const QueryPlan, filters: []const query.Filter) bool {
        if (plan.index_strategy != .points) return false;
        if (plan.filter_index) |idx| {
            return switch (filters[idx]) {
                .in => true,
                else => false,
            };
        }
        return false;
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
        limitLeft: ?u64 = null,
        offsetLeft: u64 = 0,
        next: *const fn (*ListIterator) error{ OutOfMemory, ScanError }!?BSONDocument = nextUnfetched,

        fn ensureIndexIterator(self: *ListIterator) error{ScanError}!*Index.RangeIterator {
            if (Bucket.planUsesPointStrategy(&self.plan, self.query.filters)) {
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

            if (self.query.sector) |sector| {
                if (sector.offset) |offset| {
                    if (offset < resultSlice.len) {
                        const start: usize = @intCast(offset);
                        std.mem.copyForwards(BSONDocument, resultSlice, resultSlice[start..]);
                    } else {
                        resultSlice = resultSlice[0..0];
                    }
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

        pub fn nextUnfetched(self: *ListIterator) error{ OutOfMemory, ScanError }!?BSONDocument {
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
                    return bsonDoc;
                }

                if (self.offsetLeft != 0) {
                    self.offsetLeft -= 1;
                }
            }
        }

        fn nextIndex(self: *ListIterator) error{ OutOfMemory, ScanError }!?BSONDocument {
            std.debug.assert(self.plan.source == .index);
            if (self.limitLeft != null and self.limitLeft.? == 0) {
                return null;
            }
            const ally = self.ally;
            const use_points = Bucket.planUsesPointStrategy(&self.plan, self.query.filters);

            if (!use_points) {
                const iterator = try self.ensureIndexIterator();

                while (true) {
                    const loc = iterator.next() catch |err| switch (err) {
                        error.OutOfMemory => return error.OutOfMemory,
                        else => return error.ScanError,
                    } orelse return null;
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

                    return doc;
                }
            }

            if (self.plan.index == null) {
                return null;
            }

            if (self.point_value_total == 0) {
                return null;
            }

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

                return doc;
            }
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
        }
    };

    fn planQuery(self: *Bucket, q: *const query.Query) QueryPlan {
        var plan = QueryPlan{};
        var best_score: u8 = 0;

        for (q.filters, 0..) |filter, idx| {
            switch (filter) {
                .eq => |data| {
                    const index_ptr = self.indexes.get(data.path) orelse continue;
                    const candidate_bounds = QueryPlan.IndexBounds{
                        .lower = Index.RangeBound.gte(data.value),
                        .upper = Index.RangeBound.lte(data.value),
                    };
                    const score: u8 = 100;
                    const matches_current = planMatchesRange(&plan, index_ptr, data.path);
                    if (score > best_score) {
                        best_score = score;
                        plan.source = .index;
                        plan.index = index_ptr;
                        plan.filter_index = idx;
                        plan.index_path = data.path;
                        plan.bounds = candidate_bounds;
                        plan.sort_covered = false;
                        plan.index_strategy = .range;
                    } else if (matches_current) {
                        if (candidate_bounds.lower) |lower| {
                            tightenLowerBound(&plan.bounds.lower, lower);
                        }
                        if (candidate_bounds.upper) |upper| {
                            tightenUpperBound(&plan.bounds.upper, upper);
                        }
                    }
                },
                .lt => |data| {
                    const index_ptr = self.indexes.get(data.path) orelse continue;
                    const candidate_bounds = QueryPlan.IndexBounds{
                        .upper = Index.RangeBound.lt(data.value),
                    };
                    const score: u8 = 80;
                    const matches_current = planMatchesRange(&plan, index_ptr, data.path);
                    if (score > best_score) {
                        best_score = score;
                        plan.source = .index;
                        plan.index = index_ptr;
                        plan.filter_index = idx;
                        plan.index_path = data.path;
                        plan.bounds = candidate_bounds;
                        plan.sort_covered = false;
                        plan.index_strategy = .range;
                    } else if (matches_current) {
                        if (candidate_bounds.upper) |upper| {
                            tightenUpperBound(&plan.bounds.upper, upper);
                        }
                    }
                },
                .lte => |data| {
                    const index_ptr = self.indexes.get(data.path) orelse continue;
                    const candidate_bounds = QueryPlan.IndexBounds{
                        .upper = Index.RangeBound.lte(data.value),
                    };
                    const score: u8 = 80;
                    const matches_current = planMatchesRange(&plan, index_ptr, data.path);
                    if (score > best_score) {
                        best_score = score;
                        plan.source = .index;
                        plan.index = index_ptr;
                        plan.filter_index = idx;
                        plan.index_path = data.path;
                        plan.bounds = candidate_bounds;
                        plan.sort_covered = false;
                        plan.index_strategy = .range;
                    } else if (matches_current) {
                        if (candidate_bounds.upper) |upper| {
                            tightenUpperBound(&plan.bounds.upper, upper);
                        }
                    }
                },
                .gt => |data| {
                    const index_ptr = self.indexes.get(data.path) orelse continue;
                    const candidate_bounds = QueryPlan.IndexBounds{
                        .lower = Index.RangeBound.gt(data.value),
                    };
                    const score: u8 = 80;
                    const matches_current = planMatchesRange(&plan, index_ptr, data.path);
                    if (score > best_score) {
                        best_score = score;
                        plan.source = .index;
                        plan.index = index_ptr;
                        plan.filter_index = idx;
                        plan.index_path = data.path;
                        plan.bounds = candidate_bounds;
                        plan.sort_covered = false;
                        plan.index_strategy = .range;
                    } else if (matches_current) {
                        if (candidate_bounds.lower) |lower| {
                            tightenLowerBound(&plan.bounds.lower, lower);
                        }
                    }
                },
                .gte => |data| {
                    const index_ptr = self.indexes.get(data.path) orelse continue;
                    const candidate_bounds = QueryPlan.IndexBounds{
                        .lower = Index.RangeBound.gte(data.value),
                    };
                    const score: u8 = 80;
                    const matches_current = planMatchesRange(&plan, index_ptr, data.path);
                    if (score > best_score) {
                        best_score = score;
                        plan.source = .index;
                        plan.index = index_ptr;
                        plan.filter_index = idx;
                        plan.index_path = data.path;
                        plan.bounds = candidate_bounds;
                        plan.sort_covered = false;
                        plan.index_strategy = .range;
                    } else if (matches_current) {
                        if (candidate_bounds.lower) |lower| {
                            tightenLowerBound(&plan.bounds.lower, lower);
                        }
                    }
                },
                .between => |data| {
                    const lower = data.value.array.get("0") orelse continue;
                    const upper = data.value.array.get("1") orelse continue;
                    if (!isIndexableValue(lower) or !isIndexableValue(upper)) continue;

                    const index_ptr = self.indexes.get(data.path) orelse continue;
                    const candidate_bounds = QueryPlan.IndexBounds{
                        .lower = Index.RangeBound.gt(lower),
                        .upper = Index.RangeBound.lt(upper),
                    };
                    const score: u8 = 85;
                    const matches_current = planMatchesRange(&plan, index_ptr, data.path);
                    if (score > best_score) {
                        best_score = score;
                        plan.source = .index;
                        plan.index = index_ptr;
                        plan.filter_index = idx;
                        plan.index_path = data.path;
                        plan.bounds = candidate_bounds;
                        plan.sort_covered = false;
                        plan.index_strategy = .range;
                    } else if (matches_current) {
                        if (candidate_bounds.lower) |lower_bound| {
                            tightenLowerBound(&plan.bounds.lower, lower_bound);
                        }
                        if (candidate_bounds.upper) |upper_bound| {
                            tightenUpperBound(&plan.bounds.upper, upper_bound);
                        }
                    }
                },
                .in => |data| {
                    if (data.value != bson.BSONValueType.array) continue;
                    var iter = data.value.array.iter();
                    var count: usize = 0;
                    var all_indexable = true;
                    while (iter.next()) |pair| {
                        if (!isIndexableValue(pair.value)) {
                            all_indexable = false;
                            break;
                        }
                        count += 1;
                    }
                    if (!all_indexable or count == 0) continue;

                    const index_ptr = self.indexes.get(data.path) orelse continue;
                    const score: u8 = 95;
                    if (score > best_score) {
                        best_score = score;
                        plan.source = .index;
                        plan.index = index_ptr;
                        plan.filter_index = idx;
                        plan.index_path = data.path;
                        plan.bounds = .{};
                        plan.sort_covered = false;
                        plan.index_strategy = .points;
                    }
                },
                .startsWith => |data| {
                    if (data.value != bson.BSONValueType.string) continue;
                    if (!isIndexableValue(data.value)) continue;

                    const index_ptr = self.indexes.get(data.path) orelse continue;
                    const candidate_bounds = QueryPlan.IndexBounds{
                        .lower = Index.RangeBound.gte(data.value),
                    };
                    const score: u8 = 70;
                    const matches_current = planMatchesRange(&plan, index_ptr, data.path);
                    if (score > best_score) {
                        best_score = score;
                        plan.source = .index;
                        plan.index = index_ptr;
                        plan.filter_index = idx;
                        plan.index_path = data.path;
                        plan.bounds = candidate_bounds;
                        plan.sort_covered = false;
                        plan.index_strategy = .range;
                    } else if (matches_current) {
                        if (candidate_bounds.lower) |lower| {
                            tightenLowerBound(&plan.bounds.lower, lower);
                        }
                    }
                },
                .endsWith => |_| {
                    // No indexable strategy for suffix matching.
                },
                else => {},
            }
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
                        ally.free(docRaw.data);
                    }
                }
            },
            .index => {
                const index_ptr = plan.index orelse return error.ScanError;
                self.bindIndex(index_ptr);

                if (planUsesPointStrategy(plan, q.filters)) {
                    const filter_index = plan.filter_index orelse return error.ScanError;
                    const filter_ptr = &q.filters[filter_index];
                    const inFilter = switch (filter_ptr.*) {
                        .in => |*in_ref| in_ref,
                        else => return error.ScanError,
                    };

                    // Pre-size the deduplication map based on expected number of documents
                    var seen = std.AutoHashMap(u128, void).init(ally);
                    defer seen.deinit();

                    // Pre-allocate capacity to reduce rehashing
                    var value_count: usize = 0;
                    var count_iter = inFilter.value.array.iter();
                    while (count_iter.next()) |pair| {
                        if (isIndexableValue(pair.value)) value_count += 1;
                    }
                    try seen.ensureTotalCapacity(@intCast(value_count * 4)); // Estimate 4 docs per value

                    var value_iter = inFilter.value.array.iter();
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
                                ally.free(doc.buffer);
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
                            ally.free(doc.buffer);
                        }
                    }
                }
            },
        }
    }

    pub fn listIterate(self: *Bucket, arena: *std.heap.ArenaAllocator, q: query.Query) !*ListIterator {
        var ally = arena.allocator();
        const plan = self.planQuery(&q);

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
            .limitLeft = null,
            .offsetLeft = 0,
            .index = 0,
        };

        if (Bucket.planUsesPointStrategy(&rc.plan, rc.query.filters)) {
            const fi = rc.plan.filter_index.?;
            switch (rc.query.filters[fi]) {
                .in => |inFilter| {
                    var iter = inFilter.value.array.iter();
                    while (iter.next()) |pair| {
                        if (!isIndexableValue(pair.value)) continue;
                        try rc.point_values.append(ally, pair.value);
                    }
                    rc.point_value_total = rc.point_values.items.len;
                    rc.point_values_consumed = 0;

                    // Choose deduplication strategy based on number of point values
                    if (rc.point_value_total == 1) {
                        // Single point query - no deduplication needed
                        rc.point_dedup_strategy = .none;
                    } else if (rc.point_value_total == 2) {
                        // Two values - just check if same as last location (common for array fields)
                        rc.point_dedup_strategy = .check_last;
                    } else {
                        // Multiple values - use hashmap for full deduplication
                        rc.point_dedup_strategy = .use_hashmap;
                    }
                },
                else => {},
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
                const index_ptr = plan.index orelse return error.ScanError;
                self.bindIndex(index_ptr);

                if (Bucket.planUsesPointStrategy(plan, q.filters)) {
                    const filter_index = plan.filter_index orelse return error.ScanError;
                    const filter = q.filters[filter_index];
                    const in_filter = switch (filter) {
                        .in => |value| value,
                        else => return error.ScanError,
                    };

                    var seen = std.AutoHashMap(u128, void).init(ally);
                    defer seen.deinit();

                    var value_iter = in_filter.value.array.iter();
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

            self.bucket.rwlock.lock();
            var lock_released = false;
            defer {
                if (!lock_released) self.bucket.rwlock.unlock();
            }

            var page = self.bucket.loadPage(target.page_id) catch |err| switch (err) {
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
            self.bucket.writePage(page) catch |err| switch (err) {
                error.OutOfMemory => return error.OutOfMemory,
                else => return error.ScanError,
            };

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

            self.bucket.rwlock.unlock();
            lock_released = true;

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

                _ = self.bucket.insert(insert_doc) catch |err| switch (err) {
                    error.OutOfMemory => return error.OutOfMemory,
                    error.DuplicateKey => return error.DuplicateKey,
                    else => return error.ScanError,
                };
            }

            self.current_doc = null;
            _ = self.arena.reset(.retain_capacity);
            self.index += 1;
        }

        pub fn close(self: *TransformIterator) !void {
            const allocator = self.bucket.allocator;

            if (!self.bucket.in_memory and self.bucket.autoVaccuum and self.bucket.header.deleted_count > self.bucket.header.doc_count) {
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
        var plan = self.planQuery(&q);

        var target_list = std.ArrayList(DocumentLocation){};
        defer target_list.deinit(arena.allocator());

        self.rwlock.lockShared();
        defer self.rwlock.unlockShared();
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
            .arena = arena,
            .ally = arena.allocator(),
            .query = q,
            .plan = plan,
            .targets = targets_slice,
            .index = 0,
            .current_doc = null,
            .owns_arena = false,
        };

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

        if (q.sector) |sector| {
            if (sector.offset) |offset| {
                if (offset < resultSlice.len) {
                    const start: usize = @intCast(offset);
                    std.mem.copyForwards(BSONDocument, resultSlice, resultSlice[start..]);
                } else {
                    resultSlice = resultSlice[0..0];
                }
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

    pub fn delete(self: *Bucket, q: query.Query) !void {
        // For now, we'll just print the document
        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const allocator = arena.allocator();
        var locations = std.ArrayList(DocumentMeta){};
        var iterator = try ScanIterator.init(self, allocator);
        self.rwlock.lockShared();

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
        self.rwlock.unlockShared();
        // std.debug.print("iterator state: {any}\n", .{iterator.offset});
        self.rwlock.lock();
        defer self.rwlock.unlock();
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
            var page = try self.loadPage(location.page_id);
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

        if (!self.in_memory and self.header.deleted_count > self.header.doc_count and self.autoVaccuum) {
            // If all documents are deleted, reset the deleted count
            try self.vacuum();
        }
    }

    pub fn vacuum(self: *Bucket) !void {
        if (self.in_memory) {
            return;
        }

        const tempFileName = try std.fmt.allocPrint(self.allocator, "{s}-temp", .{self.path});
        defer self.allocator.free(tempFileName);
        const cache_capacity = self.pageCache.capacity;
        var newBucket = try Bucket.openFileWithOptions(self.allocator, tempFileName, .{
            .page_cache_capacity = cache_capacity,
            .autoVaccuum = self.autoVaccuum,
        });
        defer newBucket.deinit();
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
        // Encryption is strict whenever enabled.
        self.encryption_key = null;
        self.encryption_key_version = 0;
        self.encryption_keys = std.AutoHashMap(u8, [encryption.KEY_SIZE]u8).init(self.allocator);
        self.pageCache = PageCache.init(self.allocator, cache_capacity);
        // Reinitialize and reload indexes from the meta page
        self.indexes = .init(self.allocator);
        // Reinitialize dirty_pages for replication tracking
        self.dirty_pages = .init(self.allocator);
        const meta = try self.loadPage(0);
        try self.loadIndices(meta);
    }

    pub fn deinit(self: *Bucket) void {
        // Flush any pending writes before cleanup
        self.flush() catch {
            // Ignore flush errors during cleanup
        };

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

        // Free dirty pages list (only if it has been allocated)
        // Cleanup dirty pages set
        self.dirty_pages.deinit();

        // Securely zero out encryption key
        if (self.encryption_key) |*key| {
            encryption.zeroKey(key);
        }

        var key_it = self.encryption_keys.iterator();
        while (key_it.next()) |entry| {
            var k = entry.value_ptr.*;
            encryption.zeroKey(&k);
            // Best-effort overwrite the stored value too
            entry.value_ptr.* = [_]u8{0} ** encryption.KEY_SIZE;
        }
        self.encryption_keys.deinit();

        // Free path
        self.allocator.free(self.path);

        // Close file last
        if (self.file) |*file| {
            file.close();
            self.file = null;
        }
    }

    /// Enable encryption by deriving a key from passphrase
    /// Must be called before creating/opening the bucket for encryption to work
    /// For existing buckets: all future page writes will be encrypted
    /// Note: existing pages remain unencrypted until rewritten
    pub fn setEncryptionPassphrase(self: *Bucket, passphrase: []const u8) !void {
        // Encryption is always strict: an encrypted bucket must not contain plaintext pages.
        // To avoid creating mixed-mode buckets, only allow enabling encryption at creation time
        // (before any pages exist). For already-encrypted buckets this function is used to
        // derive/install the key from the stored salt.
        if (!self.header.isEncrypted() and self.header.page_count != 0) {
            return error.EncryptionMustBeConfiguredOnOpen;
        }

        // Generate salt or use existing one
        var salt: [encryption.SALT_SIZE]u8 = undefined;

        if (self.header.isEncrypted()) {
            // Use existing salt
            salt = self.header.encryption_salt;
        } else {
            // Generate new salt and mark bucket as encrypted
            salt = encryption.generateSalt();
            self.header.setEncrypted(salt);
            try self.flushHeader();
        }

        // Derive key from passphrase
        const key = try encryption.deriveKey(self.allocator, passphrase, salt);
        self.encryption_key = key;
        self.encryption_key_version = 0;

        // Keep version 0 in the keyring too (helps when callers switch to keyring APIs later).
        try self.encryption_keys.put(0, key);
    }

    /// Set encryption key directly (useful for testing or when key is managed externally)
    pub fn setEncryptionKey(self: *Bucket, key: [encryption.KEY_SIZE]u8, key_version: u8) !void {
        if (!self.header.isEncrypted() and self.header.page_count != 0) {
            return error.EncryptionMustBeConfiguredOnOpen;
        }

        self.encryption_key = key;
        self.encryption_key_version = key_version;

        try self.encryption_keys.put(key_version, key);

        if (!self.header.isEncrypted()) {
            // Generate salt for header even though key is provided directly
            const salt = encryption.generateSalt();
            self.header.setEncrypted(salt);
            try self.flushHeader();
        }
    }

    pub fn setEncryptionKeyring(self: *Bucket, keys: []const EncryptionKeyEntry, active_version: u8) !void {
        if (!self.header.isEncrypted() and self.header.page_count != 0) {
            return error.EncryptionMustBeConfiguredOnOpen;
        }

        // Clear existing keys
        self.clearEncryptionKeys();

        for (keys) |entry| {
            try self.encryption_keys.put(entry.version, entry.key);
        }

        const active = self.encryption_keys.get(active_version) orelse return BucketInitErrors.KeyVersionNotFound;
        self.encryption_key = active;
        self.encryption_key_version = active_version;

        var header_changed = false;
        if (!self.header.isEncrypted()) {
            const salt = encryption.generateSalt();
            self.header.setEncrypted(salt);
            header_changed = true;
        }
        if (header_changed) {
            try self.flushHeader();
        }
    }

    pub fn setActiveEncryptionKeyVersion(self: *Bucket, version: u8) !void {
        const k = self.encryption_keys.get(version) orelse return BucketInitErrors.KeyVersionNotFound;
        self.encryption_key = k;
        self.encryption_key_version = version;
    }

    /// Check if encryption is enabled on this bucket
    pub fn isEncrypted(self: *const Bucket) bool {
        return self.encryption_key != null;
    }

    /// Clear encryption key (does not decrypt existing pages)
    pub fn clearEncryptionKey(self: *Bucket) void {
        if (self.encryption_key) |*key| {
            encryption.zeroKey(key);
            self.encryption_key = null;
        }
    }

    pub fn clearEncryptionKeys(self: *Bucket) void {
        self.clearEncryptionKey();
        var it = self.encryption_keys.iterator();
        while (it.next()) |entry| {
            var k = entry.value_ptr.*;
            encryption.zeroKey(&k);
            entry.value_ptr.* = [_]u8{0} ** encryption.KEY_SIZE;
        }
        self.encryption_keys.clearRetainingCapacity();
    }
};

const testing = std.testing;

test "Bucket.TransformIterator updates document" {
    var bucket_arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer bucket_arena.deinit();
    const allocator = bucket_arena.allocator();

    var bucket = try Bucket.init(allocator, "transform-update.bucket");
    defer {
        bucket.deinit();
        platform.deleteFile("transform-update.bucket") catch {};
    }

    var insert_doc = try bson.BSONDocument.fromJSON(allocator,
        \\{
        \\  "name": "Alice",
        \\  "age": 37
        \\}
    );
    defer insert_doc.deinit(allocator);
    _ = try bucket.insert(insert_doc);

    var query_arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer query_arena.deinit();
    const q_alloc = query_arena.allocator();

    var query_doc = try bson.BSONDocument.fromJSON(q_alloc,
        \\{
        \\  "query": {
        \\    "name": "Alice"
        \\  }
        \\}
    );
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

    var updated_doc = try bson.BSONDocument.fromJSON(q_alloc,
        \\{
        \\  "name": "Alice",
        \\  "age": 42
        \\}
    );
    defer updated_doc.deinit(q_alloc);

    try iter.transform(&updated_doc);

    const after = try iter.data();
    try testing.expect(after == null);

    var check_query_doc = try bson.BSONDocument.fromJSON(q_alloc,
        \\{
        \\  "query": {
        \\    "name": "Alice"
        \\  },
        \\  "sector": {}
        \\}
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

    var bucket = try Bucket.init(allocator, "transform-delete.bucket");
    defer {
        bucket.deinit();
        platform.deleteFile("transform-delete.bucket") catch {};
    }

    var insert_doc = try bson.BSONDocument.fromJSON(allocator,
        \\{
        \\  "name": "Bob",
        \\  "age": 30
        \\}
    );
    defer insert_doc.deinit(allocator);
    _ = try bucket.insert(insert_doc);

    var query_arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer query_arena.deinit();
    const q_alloc = query_arena.allocator();

    var query_doc = try bson.BSONDocument.fromJSON(q_alloc,
        \\{
        \\  "query": {
        \\    "name": "Bob"
        \\  }
        \\}
    );
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

    var check_query_doc = try bson.BSONDocument.fromJSON(q_alloc,
        \\{
        \\  "query": {
        \\    "name": "Bob"
        \\  }
        \\}
    );
    defer check_query_doc.deinit(q_alloc);

    var check_query = try query.Query.parse(q_alloc, check_query_doc);
    defer check_query.deinit(q_alloc);

    var list_arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer list_arena.deinit();
    var list_iter = try bucket.listIterate(&list_arena, check_query);

    const remaining = try list_iter.next(list_iter);
    try testing.expect(remaining == null);
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
    const doc = try bson.BSONDocument.fromJSON(allocator,
        \\{
        \\  "name": "Alice",
        \\  "age": 37
        \\}
    );

    // Insert the document into the bucket
    _ = try bucket.insert(doc);

    const q1 = try query.Query.parse(allocator, try bson.BSONDocument.fromJSON(allocator,
        \\{
        \\  "sector": {}
        \\}
    ));

    var res1list = std.ArrayList(BSONDocument){};
    defer res1list.deinit(allocator);
    var res1iter = try bucket.listIterate(&arena, q1);
    while (try res1iter.next(res1iter)) |docItem| {
        try res1list.append(allocator, docItem);
    }
    const res1 = res1list.items;

    try testing.expect(res1.len == 1);
    try testing.expectEqualStrings(res1[0].get("name").?.string.value, "Alice");

    const q2 = try query.Query.parse(allocator, try bson.BSONDocument.fromJSON(allocator,
        \\{
        \\  "query": {"name": "Alice"}
        \\}
    ));

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

test "Bucket.dump supports :memory: storage" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var in_mem_bucket = try Bucket.init(allocator, ":memory:");
    defer in_mem_bucket.deinit();

    const doc = try bson.BSONDocument.fromJSON(allocator,
        \\{
        \\  "name": "InMemory"
        \\}
    );
    _ = try in_mem_bucket.insert(doc);

    const dump_path = "memory_dump.bucket";
    defer platform.deleteFile(dump_path) catch |err| {
        std.debug.print("Failed to delete dump file: {any}\n", .{err});
    };

    try in_mem_bucket.dump(dump_path);

    var disk_bucket = try Bucket.init(allocator, dump_path);
    defer disk_bucket.deinit();

    const q = try query.Query.parse(allocator, try bson.BSONDocument.fromJSON(allocator,
        \\{
        \\  "sector": {}
        \\}
    ));

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

    const alice_scores_pairs = [_]bson.BSONKeyValuePair{
        bson.BSONKeyValuePair.init("0", bson.BSONValue{ .int32 = .{ .value = 5 } }),
        bson.BSONKeyValuePair.init("1", bson.BSONValue{ .int32 = .{ .value = 7 } }),
    };
    var alice_scores = try bson.BSONDocument.fromPairs(allocator, @constCast(alice_scores_pairs[0..]));
    defer alice_scores.deinit(allocator);

    const bob_scores_pairs = [_]bson.BSONKeyValuePair{
        bson.BSONKeyValuePair.init("0", bson.BSONValue{ .int32 = .{ .value = 9 } }),
    };
    var bob_scores = try bson.BSONDocument.fromPairs(allocator, @constCast(bob_scores_pairs[0..]));
    defer bob_scores.deinit(allocator);

    const dora_scores_pairs = [_]bson.BSONKeyValuePair{
        bson.BSONKeyValuePair.init("0", bson.BSONValue{ .int32 = .{ .value = 7 } }),
    };
    var dora_scores = try bson.BSONDocument.fromPairs(allocator, @constCast(dora_scores_pairs[0..]));
    defer dora_scores.deinit(allocator);

    const doc_pairs = [_][]const bson.BSONKeyValuePair{
        &[_]bson.BSONKeyValuePair{
            bson.BSONKeyValuePair.init("name", bson.BSONValue{ .string = .{ .value = "Alice" } }),
            bson.BSONKeyValuePair.init("age", bson.BSONValue{ .int32 = .{ .value = 30 } }),
            bson.BSONKeyValuePair.init("scores", bson.BSONValue{ .array = alice_scores }),
        },
        &[_]bson.BSONKeyValuePair{
            bson.BSONKeyValuePair.init("name", bson.BSONValue{ .string = .{ .value = "Bob" } }),
            bson.BSONKeyValuePair.init("age", bson.BSONValue{ .int32 = .{ .value = 40 } }),
            bson.BSONKeyValuePair.init("scores", bson.BSONValue{ .array = bob_scores }),
        },
        &[_]bson.BSONKeyValuePair{
            bson.BSONKeyValuePair.init("name", bson.BSONValue{ .string = .{ .value = "Carol" } }),
            bson.BSONKeyValuePair.init("age", bson.BSONValue{ .int32 = .{ .value = 50 } }),
        },
        &[_]bson.BSONKeyValuePair{
            bson.BSONKeyValuePair.init("name", bson.BSONValue{ .string = .{ .value = "Dora" } }),
            bson.BSONKeyValuePair.init("age", bson.BSONValue{ .int32 = .{ .value = 35 } }),
            bson.BSONKeyValuePair.init("scores", bson.BSONValue{ .array = dora_scores }),
        },
    };

    for (doc_pairs) |pairs| {
        var doc = try bson.BSONDocument.fromPairs(allocator, @constCast(pairs));
        _ = try bucket.insert(doc);
        doc.deinit(allocator);
    }
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

    const age_filter_pairs = [_]bson.BSONKeyValuePair{
        bson.BSONKeyValuePair.init("age", bson.BSONValue{ .int32 = .{ .value = 30 } }),
    };
    var age_filter_doc = try bson.BSONDocument.fromPairs(allocator, @constCast(age_filter_pairs[0..]));
    defer age_filter_doc.deinit(allocator);

    const root_pairs = [_]bson.BSONKeyValuePair{
        bson.BSONKeyValuePair.init("query", bson.BSONValue{ .document = age_filter_doc }),
    };
    var qdoc = try bson.BSONDocument.fromPairs(allocator, @constCast(root_pairs[0..]));
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

    const name_prefix_pairs = [_]bson.BSONKeyValuePair{
        bson.BSONKeyValuePair.init("$startsWith", bson.BSONValue{ .string = .{ .value = "A" } }),
    };
    var name_prefix_doc = try bson.BSONDocument.fromPairs(allocator, @constCast(name_prefix_pairs[0..]));
    defer name_prefix_doc.deinit(allocator);

    const name_filter_pairs = [_]bson.BSONKeyValuePair{
        bson.BSONKeyValuePair.init("name", bson.BSONValue{ .document = name_prefix_doc }),
    };
    var name_filter_doc = try bson.BSONDocument.fromPairs(allocator, @constCast(name_filter_pairs[0..]));
    defer name_filter_doc.deinit(allocator);

    const root_pairs = [_]bson.BSONKeyValuePair{
        bson.BSONKeyValuePair.init("query", bson.BSONValue{ .document = name_filter_doc }),
    };
    var qdoc = try bson.BSONDocument.fromPairs(allocator, @constCast(root_pairs[0..]));
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

    const age_range_ops = [_]bson.BSONKeyValuePair{
        bson.BSONKeyValuePair.init("$gte", bson.BSONValue{ .int32 = .{ .value = 35 } }),
        bson.BSONKeyValuePair.init("$lte", bson.BSONValue{ .int32 = .{ .value = 40 } }),
    };
    var age_range_doc = try bson.BSONDocument.fromPairs(allocator, @constCast(age_range_ops[0..]));
    defer age_range_doc.deinit(allocator);

    const age_filter_pairs = [_]bson.BSONKeyValuePair{
        bson.BSONKeyValuePair.init("age", bson.BSONValue{ .document = age_range_doc }),
    };
    var age_filter_doc = try bson.BSONDocument.fromPairs(allocator, @constCast(age_filter_pairs[0..]));
    defer age_filter_doc.deinit(allocator);

    const root_pairs = [_]bson.BSONKeyValuePair{
        bson.BSONKeyValuePair.init("query", bson.BSONValue{ .document = age_filter_doc }),
    };
    var qdoc = try bson.BSONDocument.fromPairs(allocator, @constCast(root_pairs[0..]));
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

    const in_values_pairs = [_]bson.BSONKeyValuePair{
        bson.BSONKeyValuePair.init("0", bson.BSONValue{ .int32 = .{ .value = 30 } }),
        bson.BSONKeyValuePair.init("1", bson.BSONValue{ .int32 = .{ .value = 40 } }),
    };
    var in_values_doc = try bson.BSONDocument.fromPairs(allocator, @constCast(in_values_pairs[0..]));
    defer in_values_doc.deinit(allocator);

    const age_in_pairs = [_]bson.BSONKeyValuePair{
        bson.BSONKeyValuePair.init("$in", bson.BSONValue{ .array = in_values_doc }),
    };
    var age_in_doc = try bson.BSONDocument.fromPairs(allocator, @constCast(age_in_pairs[0..]));
    defer age_in_doc.deinit(allocator);

    const age_filter_pairs = [_]bson.BSONKeyValuePair{
        bson.BSONKeyValuePair.init("age", bson.BSONValue{ .document = age_in_doc }),
    };
    var age_filter_doc = try bson.BSONDocument.fromPairs(allocator, @constCast(age_filter_pairs[0..]));
    defer age_filter_doc.deinit(allocator);

    const root_pairs = [_]bson.BSONKeyValuePair{
        bson.BSONKeyValuePair.init("query", bson.BSONValue{ .document = age_filter_doc }),
    };
    var qdoc = try bson.BSONDocument.fromPairs(allocator, @constCast(root_pairs[0..]));
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

test "Bucket.indexed query between uses range" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = std.testing.allocator;
    platform.deleteFile("index-query-between.bucket") catch {};
    var bucket = try Bucket.init(allocator, "index-query-between.bucket");
    try setupIndexQueryBucket(&bucket, allocator);
    defer bucket.deinit();
    defer platform.deleteFile("index-query-between.bucket") catch {};

    const between_values_pairs = [_]bson.BSONKeyValuePair{
        bson.BSONKeyValuePair.init("0", bson.BSONValue{ .int32 = .{ .value = 29 } }),
        bson.BSONKeyValuePair.init("1", bson.BSONValue{ .int32 = .{ .value = 38 } }),
    };
    var between_values_doc = try bson.BSONDocument.fromPairs(allocator, @constCast(between_values_pairs[0..]));
    defer between_values_doc.deinit(allocator);

    const age_between_pairs = [_]bson.BSONKeyValuePair{
        bson.BSONKeyValuePair.init("$between", bson.BSONValue{ .array = between_values_doc }),
    };
    var age_between_doc = try bson.BSONDocument.fromPairs(allocator, @constCast(age_between_pairs[0..]));
    defer age_between_doc.deinit(allocator);

    const age_filter_pairs = [_]bson.BSONKeyValuePair{
        bson.BSONKeyValuePair.init("age", bson.BSONValue{ .document = age_between_doc }),
    };
    var age_filter_doc = try bson.BSONDocument.fromPairs(allocator, @constCast(age_filter_pairs[0..]));
    defer age_filter_doc.deinit(allocator);

    const root_pairs = [_]bson.BSONKeyValuePair{
        bson.BSONKeyValuePair.init("query", bson.BSONValue{ .document = age_filter_doc }),
    };
    var qdoc = try bson.BSONDocument.fromPairs(allocator, @constCast(root_pairs[0..]));
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

test "Bucket.indexed query in scores avoids duplicates" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = std.testing.allocator;
    platform.deleteFile("index-query-scores.bucket") catch {};
    var bucket = try Bucket.init(allocator, "index-query-scores.bucket");
    try setupIndexQueryBucket(&bucket, allocator);
    defer bucket.deinit();
    defer platform.deleteFile("index-query-scores.bucket") catch {};

    const score_values_pairs = [_]bson.BSONKeyValuePair{
        bson.BSONKeyValuePair.init("0", bson.BSONValue{ .int32 = .{ .value = 7 } }),
    };
    var score_values_doc = try bson.BSONDocument.fromPairs(allocator, @constCast(score_values_pairs[0..]));
    defer score_values_doc.deinit(allocator);

    const scores_in_pairs = [_]bson.BSONKeyValuePair{
        bson.BSONKeyValuePair.init("$in", bson.BSONValue{ .array = score_values_doc }),
    };
    var scores_in_doc = try bson.BSONDocument.fromPairs(allocator, @constCast(scores_in_pairs[0..]));
    defer scores_in_doc.deinit(allocator);

    const scores_filter_pairs = [_]bson.BSONKeyValuePair{
        bson.BSONKeyValuePair.init("scores", bson.BSONValue{ .document = scores_in_doc }),
    };
    var scores_filter_doc = try bson.BSONDocument.fromPairs(allocator, @constCast(scores_filter_pairs[0..]));
    defer scores_filter_doc.deinit(allocator);

    const root_pairs_scores = [_]bson.BSONKeyValuePair{
        bson.BSONKeyValuePair.init("query", bson.BSONValue{ .document = scores_filter_doc }),
    };
    var qdoc = try bson.BSONDocument.fromPairs(allocator, @constCast(root_pairs_scores[0..]));
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
    const jsonBuf = try testing.allocator.alloc(u8, 300);
    defer testing.allocator.free(jsonBuf);
    for (0..900) |i| {
        const docMany = try bson.BSONDocument.fromJSON(allocator, try std.fmt.bufPrint(jsonBuf,
            \\{{
            \\  "name": "test-{d}",
            \\  "age": 10
            \\}}
        , .{i}));

        _ = try bucket.insert(docMany);
        @memset(jsonBuf, 0);
        allocator.free(docMany.buffer);
    }

    const q1 = try query.Query.parse(allocator, try bson.BSONDocument.fromJSON(allocator,
        \\{
        \\  "sector": {}
        \\}
    ));

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
    const doc = try bson.BSONDocument.fromJSON(allocator,
        \\ {
        \\  "name": "Alice",
        \\  "age": "delete me"
        \\ }
    );

    // Insert the document into the bucket
    _ = try bucket.insert(doc);

    const listQ = try query.Query.parse(allocator, try bson.BSONDocument.fromJSON(allocator,
        \\{
        \\  "query": {}
        \\}
    ));

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

    const deleteQ = try query.Query.parse(allocator, try bson.BSONDocument.fromJSON(allocator,
        \\{
        \\  "query": {"name": "Alice"}
        \\}
    ));
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
    const doc = try bson.BSONDocument.fromJSON(allocator,
        \\ {
        \\  "name": "Alice",
        \\  "age": "delete me"
        \\ }
    );

    const docGood = try bson.BSONDocument.fromJSON(allocator,
        \\ {
        \\  "name": "not Alice",
        \\  "age": "delete me"
        \\ }
    );

    // Insert the document into the bucket
    _ = try bucket.insert(doc);
    _ = try bucket.insert(docGood);

    const listQ = try query.Query.parse(allocator, try bson.BSONDocument.fromJSON(allocator,
        \\{
        \\  "query": {}
        \\}
    ));

    var docsList = std.ArrayList(BSONDocument){};
    defer docsList.deinit(allocator);
    var docsIter = try bucket.listIterate(&arena, listQ);
    while (try docsIter.next(docsIter)) |docItem| {
        try docsList.append(allocator, docItem);
    }

    const deleteQ = try query.Query.parse(allocator, try bson.BSONDocument.fromJSON(allocator,
        \\{
        \\  "query": {"name": "Alice"}
        \\}
    ));

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
        const pairs = [_]bson.BSONKeyValuePair{
            bson.BSONKeyValuePair.init("name", bson.BSONValue{ .string = .{ .value = td.name } }),
            bson.BSONKeyValuePair.init("score", bson.BSONValue{ .int32 = .{ .value = td.score } }),
        };
        var doc = try bson.BSONDocument.fromPairs(allocator, @constCast(pairs[0..]));
        _ = try bucket.insert(doc);
        doc.deinit(allocator);
    }

    // Test 1: Query with descending sort should use the reverse index (sort_covered = true)
    {
        var query_doc = try bson.BSONDocument.fromJSON(allocator,
            \\{
            \\  "sort": {"desc": "score"}
            \\}
        );
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
        var query_doc = try bson.BSONDocument.fromJSON(allocator,
            \\{
            \\  "sort": {"asc": "score"}
            \\}
        );
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
        var query_doc = try bson.BSONDocument.fromJSON(allocator,
            \\{
            \\  "query": {
            \\    "score": {"$gte": 20, "$lte": 40}
            \\  },
            \\  "sort": {"desc": "score"}
            \\}
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

test "Bucket.replication with page streaming" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    // Create primary bucket
    var primary = try Bucket.init(allocator, "replication-primary.bucket");
    defer primary.deinit();
    defer platform.deleteFile("replication-primary.bucket") catch {};

    // Create replica bucket
    var replica = try Bucket.init(allocator, "replication-replica.bucket");
    defer replica.deinit();
    defer platform.deleteFile("replication-replica.bucket") catch {};

    // Context to track replicated pages
    const ReplicationContext = struct {
        replica_bucket: *Bucket,
        pages_replicated: usize = 0,
        last_page_id: u64 = 0,
    };

    var ctx = ReplicationContext{
        .replica_bucket = &replica,
    };

    // Define callback that applies pages to replica
    const replicationCallback = struct {
        fn callback(
            context: ?*anyopaque,
            page_data: [*]const u8,
            data_size: u32,
            page_count: u32,
        ) callconv(.c) u8 {
            const self: *ReplicationContext = @ptrCast(@alignCast(context.?));
            const page_slice = page_data[0..data_size];

            // Apply page to replica
            self.replica_bucket.applyReplicatedBatch(page_slice, page_count) catch |err| {
                std.debug.print("Failed to apply replicated page: {any}\n", .{err});
                return 1;
            };

            self.pages_replicated += page_count;
            return 0;
        }
    }.callback;

    // Set replication callback on primary
    primary.replication_callback = replicationCallback;
    primary.replication_context = &ctx;

    // Insert documents into primary
    const test_docs = [_]struct { name: []const u8, age: i32 }{
        .{ .name = "Alice", .age = 30 },
        .{ .name = "Bob", .age = 25 },
        .{ .name = "Carol", .age = 35 },
    };

    for (test_docs) |td| {
        const pairs = [_]bson.BSONKeyValuePair{
            bson.BSONKeyValuePair.init("name", bson.BSONValue{ .string = .{ .value = td.name } }),
            bson.BSONKeyValuePair.init("age", bson.BSONValue{ .int32 = .{ .value = td.age } }),
        };
        var doc = try bson.BSONDocument.fromPairs(allocator, @constCast(pairs[0..]));
        _ = try primary.insert(doc);
        doc.deinit(allocator);
    }

    // Force flush to trigger replication
    try primary.flush();

    // Verify pages were replicated
    try testing.expect(ctx.pages_replicated > 0);
    std.debug.print("Replicated {d} pages\n", .{ctx.pages_replicated});

    // Clear replica's cache to force reading from disk
    replica.pageCache.clear(allocator);

    // Query replica to verify data was replicated
    var query_doc = try bson.BSONDocument.fromJSON(allocator,
        \\{
        \\  "sector": {}
        \\}
    );
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
        const name = doc.get("name").?.string.value;
        const age = doc.get("age").?.int32.value;

        std.debug.print("Found in replica: {s}, age: {d}\n", .{ name, age });

        // Check names before freeing the buffer
        if (std.mem.eql(u8, name, "Alice")) alice_found = true;
        if (std.mem.eql(u8, name, "Bob")) bob_found = true;
        if (std.mem.eql(u8, name, "Carol")) carol_found = true;

        docs_found += 1;
        allocator.free(doc.buffer);
    }

    // Verify all documents were replicated
    try testing.expectEqual(@as(usize, 3), docs_found);

    // Verify expected names exist
    try testing.expect(alice_found);
    try testing.expect(bob_found);
    try testing.expect(carol_found);

    // Test incremental replication: insert more docs
    ctx.pages_replicated = 0; // Reset counter

    const more_docs = [_]struct { name: []const u8, age: i32 }{
        .{ .name = "Dave", .age = 28 },
        .{ .name = "Eve", .age = 32 },
    };

    for (more_docs) |td| {
        const pairs = [_]bson.BSONKeyValuePair{
            bson.BSONKeyValuePair.init("name", bson.BSONValue{ .string = .{ .value = td.name } }),
            bson.BSONKeyValuePair.init("age", bson.BSONValue{ .int32 = .{ .value = td.age } }),
        };
        var doc = try bson.BSONDocument.fromPairs(allocator, @constCast(pairs[0..]));
        _ = try primary.insert(doc);
        doc.deinit(allocator);
    }

    try primary.flush();

    // Verify incremental pages were replicated
    try testing.expect(ctx.pages_replicated > 0);
    std.debug.print("Incrementally replicated {d} more pages\n", .{ctx.pages_replicated});

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
    std.debug.print("Replication test completed successfully!\n", .{});
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
    const pairs_a = [_]bson.BSONKeyValuePair{
        bson.BSONKeyValuePair.init("name", bson.BSONValue{ .string = .{ .value = "Zed" } }),
        bson.BSONKeyValuePair.init("age", bson.BSONValue{ .int32 = .{ .value = 42 } }),
    };
    var doc_a = try bson.BSONDocument.fromPairs(allocator, @constCast(pairs_a[0..]));
    _ = try bucket.insert(doc_a);
    doc_a.deinit(allocator);

    const pairs_b = [_]bson.BSONKeyValuePair{
        bson.BSONKeyValuePair.init("name", bson.BSONValue{ .string = .{ .value = "Other" } }),
        bson.BSONKeyValuePair.init("age", bson.BSONValue{ .int32 = .{ .value = 30 } }),
    };
    var doc_b = try bson.BSONDocument.fromPairs(allocator, @constCast(pairs_b[0..]));
    _ = try bucket.insert(doc_b);
    doc_b.deinit(allocator);

    // Build equality query on age to ensure index plan
    const age_eq_pairs = [_]bson.BSONKeyValuePair{
        bson.BSONKeyValuePair.init("age", bson.BSONValue{ .int32 = .{ .value = 42 } }),
    };
    var age_eq_doc = try bson.BSONDocument.fromPairs(allocator, @constCast(age_eq_pairs[0..]));
    defer age_eq_doc.deinit(allocator);

    const root_pairs = [_]bson.BSONKeyValuePair{
        bson.BSONKeyValuePair.init("query", bson.BSONValue{ .document = age_eq_doc }),
    };
    var qdoc = try bson.BSONDocument.fromPairs(allocator, @constCast(root_pairs[0..]));
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
    const del_pairs = [_]bson.BSONKeyValuePair{
        bson.BSONKeyValuePair.init("name", bson.BSONValue{ .string = .{ .value = "Zed" } }),
    };
    var del_doc = try bson.BSONDocument.fromPairs(allocator, @constCast(del_pairs[0..]));
    defer del_doc.deinit(allocator);
    const del_root_pairs = [_]bson.BSONKeyValuePair{
        bson.BSONKeyValuePair.init("query", bson.BSONValue{ .document = del_doc }),
    };
    var del_qdoc = try bson.BSONDocument.fromPairs(allocator, @constCast(del_root_pairs[0..]));
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

test "Bucket encryption: insert, close, reopen with passphrase" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const file_name = "encrypted.bucket";
    defer platform.deleteFile(file_name) catch {};

    const passphrase = "my-secret-password";

    // Create encrypted bucket
    {
        var bucket = try Bucket.initWithPassphrase(allocator, file_name, passphrase);
        defer bucket.deinit();

        try testing.expect(bucket.header.isEncrypted());

        // Insert some documents
        const pairs1 = [_]bson.BSONKeyValuePair{
            bson.BSONKeyValuePair.init("name", bson.BSONValue{ .string = .{ .value = "Alice" } }),
            bson.BSONKeyValuePair.init("secret", bson.BSONValue{ .string = .{ .value = "classified" } }),
        };
        var doc1 = try bson.BSONDocument.fromPairs(allocator, @constCast(pairs1[0..]));
        defer doc1.deinit(allocator);
        _ = try bucket.insert(doc1);

        const pairs2 = [_]bson.BSONKeyValuePair{
            bson.BSONKeyValuePair.init("name", bson.BSONValue{ .string = .{ .value = "Bob" } }),
            bson.BSONKeyValuePair.init("secret", bson.BSONValue{ .string = .{ .value = "top-secret" } }),
        };
        var doc2 = try bson.BSONDocument.fromPairs(allocator, @constCast(pairs2[0..]));
        defer doc2.deinit(allocator);
        _ = try bucket.insert(doc2);

        try bucket.flush();
    }

    // Reopen bucket and verify we can decrypt with correct passphrase
    {
        var bucket = try Bucket.initWithPassphrase(allocator, file_name, passphrase);
        defer bucket.deinit();

        try testing.expect(bucket.header.isEncrypted());

        // Query all documents
        const q_pairs = [_]bson.BSONKeyValuePair{};
        var q_doc = try bson.BSONDocument.fromPairs(allocator, @constCast(q_pairs[0..]));
        defer q_doc.deinit(allocator);
        const root_pairs = [_]bson.BSONKeyValuePair{
            bson.BSONKeyValuePair.init("query", bson.BSONValue{ .document = q_doc }),
        };
        var root_doc = try bson.BSONDocument.fromPairs(allocator, @constCast(root_pairs[0..]));
        defer root_doc.deinit(allocator);
        var q = try query.Query.parse(allocator, root_doc);
        defer q.deinit(allocator);

        const results = try bucket.list(arena.allocator(), q);
        try testing.expectEqual(@as(usize, 2), results.len);

        // Verify we can read the secret data
        const alice = results[0];
        const alice_secret = alice.get("secret").?.string.value;
        try testing.expectEqualStrings("classified", alice_secret);

        const bob = results[1];
        const bob_secret = bob.get("secret").?.string.value;
        try testing.expectEqualStrings("top-secret", bob_secret);
    }

    std.debug.print("Encryption test passed!\n", .{});
}

test "Bucket encryption: wrong passphrase fails to decrypt" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const file_name = "encrypted-wrong-pass.bucket";
    defer platform.deleteFile(file_name) catch {};

    const correct_passphrase = "correct-password";
    const wrong_passphrase = "wrong-password";

    // Create encrypted bucket
    {
        var bucket = try Bucket.initWithPassphrase(allocator, file_name, correct_passphrase);
        defer bucket.deinit();

        const pairs = [_]bson.BSONKeyValuePair{
            bson.BSONKeyValuePair.init("data", bson.BSONValue{ .string = .{ .value = "secret" } }),
        };
        var doc = try bson.BSONDocument.fromPairs(allocator, @constCast(pairs[0..]));
        defer doc.deinit(allocator);
        _ = try bucket.insert(doc);
        try bucket.flush();
    }

    // Try to open with wrong passphrase
    {
        const result = Bucket.initWithPassphrase(allocator, file_name, wrong_passphrase);
        try testing.expectError(error.EncryptionAuthenticationFailed, result);
    }

    std.debug.print("Wrong passphrase test passed!\n", .{});
}

test "Bucket encryption: page cache stores decrypted data" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const file_name = "encrypted-cache.bucket";
    defer platform.deleteFile(file_name) catch {};

    var bucket = try Bucket.initWithPassphrase(allocator, file_name, "test-password");
    defer bucket.deinit();

    // Insert document
    const pairs = [_]bson.BSONKeyValuePair{
        bson.BSONKeyValuePair.init("value", bson.BSONValue{ .int32 = .{ .value = 42 } }),
    };
    var doc = try bson.BSONDocument.fromPairs(allocator, @constCast(pairs[0..]));
    defer doc.deinit(allocator);
    _ = try bucket.insert(doc);
    try bucket.flush();

    // Query once - decrypts and caches
    const q_pairs = [_]bson.BSONKeyValuePair{};
    var q_doc = try bson.BSONDocument.fromPairs(allocator, @constCast(q_pairs[0..]));
    defer q_doc.deinit(allocator);
    const root_pairs = [_]bson.BSONKeyValuePair{
        bson.BSONKeyValuePair.init("query", bson.BSONValue{ .document = q_doc }),
    };
    var root_doc = try bson.BSONDocument.fromPairs(allocator, @constCast(root_pairs[0..]));
    defer root_doc.deinit(allocator);
    var q = try query.Query.parse(allocator, root_doc);
    defer q.deinit(allocator);

    const results1 = try bucket.list(arena.allocator(), q);
    try testing.expectEqual(@as(usize, 1), results1.len);

    // Clear all encryption keys (active + keyring) (simulating missing key scenario)
    bucket.clearEncryptionKeys();
    try testing.expect(!bucket.isEncrypted());

    // Query again - should still work because pages are cached (decrypted)
    const results2 = try bucket.list(arena.allocator(), q);
    try testing.expectEqual(@as(usize, 1), results2.len);

    // Clear cache - now queries should fail without key
    bucket.pageCache.clear(allocator);

    // This should fail - no encryption key and cache is cold (wraps as ScanError)
    const result3 = bucket.list(arena.allocator(), q);
    try testing.expectError(error.ScanError, result3);

    std.debug.print("Cache encryption test passed!\n", .{});
}

test "Bucket compatibility: plaintext buckets remain readable" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const file_name = "compat-plaintext.bucket";
    defer platform.deleteFile(file_name) catch {};

    // Create a plaintext bucket (simulates a pre-encryption DB).
    {
        var bucket = try Bucket.init(allocator, file_name);
        defer bucket.deinit();

        try testing.expect(!bucket.header.isEncrypted());

        const pairs = [_]bson.BSONKeyValuePair{
            bson.BSONKeyValuePair.init("name", bson.BSONValue{ .string = .{ .value = "Legacy" } }),
            bson.BSONKeyValuePair.init("n", bson.BSONValue{ .int32 = .{ .value = 7 } }),
        };
        var doc = try bson.BSONDocument.fromPairs(allocator, @constCast(pairs[0..]));
        defer doc.deinit(allocator);
        _ = try bucket.insert(doc);
        try bucket.flush();
    }

    // Reopen and verify contents.
    {
        var bucket = try Bucket.init(allocator, file_name);
        defer bucket.deinit();

        try testing.expect(!bucket.header.isEncrypted());

        const q_pairs = [_]bson.BSONKeyValuePair{};
        var q_doc = try bson.BSONDocument.fromPairs(allocator, @constCast(q_pairs[0..]));
        defer q_doc.deinit(allocator);
        const root_pairs = [_]bson.BSONKeyValuePair{
            bson.BSONKeyValuePair.init("query", bson.BSONValue{ .document = q_doc }),
        };
        var root_doc = try bson.BSONDocument.fromPairs(allocator, @constCast(root_pairs[0..]));
        defer root_doc.deinit(allocator);
        var q = try query.Query.parse(allocator, root_doc);
        defer q.deinit(allocator);

        const results = try bucket.list(arena.allocator(), q);
        try testing.expectEqual(@as(usize, 1), results.len);
        try testing.expectEqualStrings("Legacy", results[0].get("name").?.string.value);
        try testing.expectEqual(@as(i32, 7), results[0].get("n").?.int32.value);
    }
}

test "Bucket compatibility: passphrase ignored for plaintext buckets" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const file_name = "compat-plaintext-with-pass.bucket";
    defer platform.deleteFile(file_name) catch {};

    // Create plaintext bucket.
    {
        var bucket = try Bucket.init(allocator, file_name);
        defer bucket.deinit();

        const pairs = [_]bson.BSONKeyValuePair{
            bson.BSONKeyValuePair.init("k", bson.BSONValue{ .string = .{ .value = "v" } }),
        };
        var doc = try bson.BSONDocument.fromPairs(allocator, @constCast(pairs[0..]));
        defer doc.deinit(allocator);
        _ = try bucket.insert(doc);
        try bucket.flush();
    }

    // Reopen using the passphrase API. This should NOT flip the bucket into encrypted mode.
    {
        var bucket = try Bucket.initWithPassphrase(allocator, file_name, "some-passphrase");
        defer bucket.deinit();

        try testing.expect(!bucket.header.isEncrypted());

        const q_pairs = [_]bson.BSONKeyValuePair{};
        var q_doc = try bson.BSONDocument.fromPairs(allocator, @constCast(q_pairs[0..]));
        defer q_doc.deinit(allocator);
        const root_pairs = [_]bson.BSONKeyValuePair{
            bson.BSONKeyValuePair.init("query", bson.BSONValue{ .document = q_doc }),
        };
        var root_doc = try bson.BSONDocument.fromPairs(allocator, @constCast(root_pairs[0..]));
        defer root_doc.deinit(allocator);
        var q = try query.Query.parse(allocator, root_doc);
        defer q.deinit(allocator);

        const results = try bucket.list(arena.allocator(), q);
        try testing.expectEqual(@as(usize, 1), results.len);
        try testing.expectEqualStrings("v", results[0].get("k").?.string.value);
    }
}

test "Bucket encryption strict: requires key to open" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const file_name = "encrypted-strict.bucket";
    defer platform.deleteFile(file_name) catch {};

    const passphrase = "strict-passphrase";

    // Create an encrypted+strict bucket from the start
    {
        var bucket = try Bucket.openFileWithOptionsAndPassphrase(allocator, file_name, passphrase, .{});
        defer bucket.deinit();

        const pairs = [_]bson.BSONKeyValuePair{
            bson.BSONKeyValuePair.init("data", bson.BSONValue{ .string = .{ .value = "secret" } }),
        };
        var doc = try bson.BSONDocument.fromPairs(allocator, @constCast(pairs[0..]));
        defer doc.deinit(allocator);
        _ = try bucket.insert(doc);
        try bucket.flush();
    }

    // Opening without a key should fail immediately (meta page is encrypted)
    {
        const result = Bucket.init(allocator, file_name);
        try testing.expectError(error.MissingEncryptionKey, result);
    }

    // Opening with the passphrase should work
    {
        var bucket = try Bucket.initWithPassphrase(allocator, file_name, passphrase);
        defer bucket.deinit();

        const q_pairs = [_]bson.BSONKeyValuePair{};
        var q_doc = try bson.BSONDocument.fromPairs(allocator, @constCast(q_pairs[0..]));
        defer q_doc.deinit(allocator);
        const root_pairs = [_]bson.BSONKeyValuePair{
            bson.BSONKeyValuePair.init("query", bson.BSONValue{ .document = q_doc }),
        };
        var root_doc = try bson.BSONDocument.fromPairs(allocator, @constCast(root_pairs[0..]));
        defer root_doc.deinit(allocator);
        var q = try query.Query.parse(allocator, root_doc);
        defer q.deinit(allocator);

        const results = try bucket.list(arena.allocator(), q);
        try testing.expectEqual(@as(usize, 1), results.len);
    }
}

test "Bucket encryption rotation: keyring supports multiple key versions" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const file_name = "encrypted-rotate.bucket";
    defer platform.deleteFile(file_name) catch {};

    const key_v1: [encryption.KEY_SIZE]u8 = [_]u8{0x11} ** encryption.KEY_SIZE;
    const key_v2: [encryption.KEY_SIZE]u8 = [_]u8{0x22} ** encryption.KEY_SIZE;

    const keys1 = [_]Bucket.EncryptionKeyEntry{
        .{ .version = 1, .key = key_v1 },
    };
    const keys_both = [_]Bucket.EncryptionKeyEntry{
        .{ .version = 1, .key = key_v1 },
        .{ .version = 2, .key = key_v2 },
    };
    const keys2 = [_]Bucket.EncryptionKeyEntry{
        .{ .version = 2, .key = key_v2 },
    };

    // Step 1: Create encrypted bucket with v1. This creates meta + _id index pages encrypted with key_version=1.
    {
        var bucket = try Bucket.openFileWithOptionsAndEncryptionKeys(allocator, file_name, keys1[0..], 1, .{});
        defer bucket.deinit();
        try bucket.flush();
    }

    // Step 2: Reopen with both keys and rotate to v2 by creating an additional index.
    // This should create new pages encrypted with key_version=2 and rewrite meta under v2.
    {
        var bucket = try Bucket.openFileWithOptionsAndEncryptionKeys(allocator, file_name, keys_both[0..], 2, .{});
        defer bucket.deinit();
        try bucket.ensureIndex("marker", .{});
        try bucket.flush();
    }

    // Step 3: Verify we have pages encrypted under both key versions.
    {
        var bucket = try Bucket.openFileWithOptionsAndEncryptionKeys(allocator, file_name, keys_both[0..], 2, .{});
        defer bucket.deinit();

        var saw_v1: bool = false;
        var saw_v2: bool = false;
        var page_id: u64 = 0;
        while (page_id < bucket.header.page_count) : (page_id += 1) {
            const p = try bucket.loadPage(page_id);
            if (!p.header.isEncrypted()) continue;
            if (p.header.key_version == 1) saw_v1 = true;
            if (p.header.key_version == 2) saw_v2 = true;
        }
        try testing.expect(saw_v1);
        try testing.expect(saw_v2);
    }

    // Step 4: Opening with only v2 should fail, since some pages (e.g. _id index) remain encrypted with v1.
    {
        const result = Bucket.openFileWithOptionsAndEncryptionKeys(allocator, file_name, keys2[0..], 2, .{});
        try testing.expectError(error.MissingEncryptionKey, result);
    }
}
