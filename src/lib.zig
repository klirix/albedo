const std = @import("std");
const testing = std.testing;
const albedo = @import("./albedo.zig");
const bson = @import("./bson.zig");
const platform = @import("./platform.zig");
const Query = albedo.Query;
const IndexOptions = @import("./bplusindex.zig").IndexOptions;
const builtin = @import("builtin");

const ally = if (builtin.is_test)
    std.testing.allocator
else if (platform.isWasm)
    std.heap.wasm_allocator
else
    std.heap.smp_allocator;

const Bucket = albedo.Bucket;
const ReplicationCursor = albedo.ReplicationCursor;
const ReplicationCursorHandle = struct {
    cursor: ReplicationCursor,
};

fn createReplicationCursorHandle(cursor: ReplicationCursor) !*ReplicationCursorHandle {
    const handle = try ally.create(ReplicationCursorHandle);
    handle.* = .{ .cursor = cursor };
    return handle;
}

const Result = enum(u8) {
    // Generic result codes
    OK = 0,
    Error = 1,
    HasData,
    EOS,

    // Specific error codes
    OutOfMemory,
    FileNotFound,
    NotFound,
    InvalidFormat,
    DuplicateKey,
    InvalidCursor,
    UnsupportedCursorQuery,
    /// The subscriber fell behind the oplog ring; must re-subscribe.
    OplogGap,
    /// The replication cursor/batch is no longer valid after a WAL reset.
    ReplicationGap,
    TransactionActive,
    InvalidTransaction,
    TransactionBusy,
};

fn mapQueryParseError(err: anyerror) Result {
    return switch (err) {
        Query.QueryParsingErrors.InvalidCursor,
        Query.QueryParsingErrors.InvalidCursorVersion,
        Query.QueryParsingErrors.InvalidCursorMode,
        Query.QueryParsingErrors.MissingCursorIndexPath,
        Query.QueryParsingErrors.InvalidCursorAnchor,
        Query.QueryParsingErrors.InvalidCursorDocId,
        Query.QueryParsingErrors.InvalidCursorPageId,
        Query.QueryParsingErrors.InvalidCursorOffset,
        => Result.InvalidCursor,
        Query.QueryParsingErrors.OutOfMemory => Result.OutOfMemory,
        else => Result.Error,
    };
}

fn mapListError(err: anyerror) Result {
    return switch (err) {
        error.InvalidCursor => Result.InvalidCursor,
        error.UnsupportedCursorQuery => Result.UnsupportedCursorQuery,
        error.OutOfMemory => Result.OutOfMemory,
        error.TransactionActive => Result.TransactionActive,
        else => Result.Error,
    };
}

fn mapTransactionError(err: anyerror) Result {
    return switch (err) {
        error.OutOfMemory => Result.OutOfMemory,
        error.TransactionActive => Result.TransactionActive,
        error.InvalidTransaction => Result.InvalidTransaction,
        error.TransactionBusy => Result.TransactionBusy,
        else => Result.Error,
    };
}

fn mapWriteError(err: anyerror) Result {
    return switch (err) {
        error.OutOfMemory => Result.OutOfMemory,
        error.DuplicateKey => Result.DuplicateKey,
        error.TransactionActive => Result.TransactionActive,
        error.InvalidTransaction => Result.InvalidTransaction,
        error.TransactionBusy => Result.TransactionBusy,
        else => Result.Error,
    };
}

fn mapReplicationError(err: anyerror) Result {
    return switch (err) {
        error.OutOfMemory => Result.OutOfMemory,
        error.InvalidCursor => Result.InvalidCursor,
        error.InvalidFormat, error.InvalidArgument => Result.InvalidFormat,
        error.ReplicationGap => Result.ReplicationGap,
        else => Result.Error,
    };
}

pub export fn albedo_open(path: [*:0]u8, out: **albedo.Bucket) Result {
    const pathProper = std.mem.span(path);
    // var gpa = std.heap.GeneralPurposeAllocator(.{}).init;
    // defer _ = gpa.deinit();
    const db = ally.create(albedo.Bucket) catch return Result.OutOfMemory;
    db.* = albedo.Bucket.init(ally, pathProper) catch {
        ally.destroy(db);
        return Result.Error;
    };
    out.* = db;
    return Result.OK;
}

pub export fn albedo_open_with_options(path: [*:0]u8, optionsBuffer: [*]u8, out: **albedo.Bucket) Result {
    const pathProper = std.mem.span(path);
    const optionsSize = std.mem.readInt(u32, optionsBuffer[0..4], .little);
    const optionsDoc = bson.BSONDocument.init(optionsBuffer[0..optionsSize]);

    var parsed = bson.fmt.parse(Bucket.OpenBucketOptions, optionsDoc, ally) catch return Result.InvalidFormat;
    defer parsed.deinit();

    const db = ally.create(albedo.Bucket) catch return Result.OutOfMemory;
    db.* = albedo.Bucket.openFileWithOptions(ally, pathProper, parsed.value) catch {
        ally.destroy(db);
        return Result.Error;
    };
    out.* = db;
    return Result.OK;
}

pub export fn albedo_close(bucket: *albedo.Bucket) Result {
    bucket.deinit();
    bucket.allocator.destroy(bucket);
    return Result.OK;
}

pub export fn albedo_insert(bucket: *albedo.Bucket, docBuffer: [*]u8) Result {
    const docSize = std.mem.readInt(u32, docBuffer[0..4], .little);
    const docBufferProper = docBuffer[0..docSize];

    const doc = bson.BSONDocument.init(docBufferProper);

    _ = bucket.insert(doc) catch |err| return mapWriteError(err);

    return Result.OK;
}

pub export fn albedo_transaction_begin(bucket: *Bucket, out: **Bucket.Transaction) Result {
    const tx = bucket.beginTransaction() catch |err| return mapTransactionError(err);
    out.* = tx;
    return Result.OK;
}

pub export fn albedo_transaction_insert(tx: *Bucket.Transaction, docBuffer: [*]u8) Result {
    const docSize = std.mem.readInt(u32, docBuffer[0..4], .little);
    const docBufferProper = docBuffer[0..docSize];
    const doc = bson.BSONDocument.init(docBufferProper);

    _ = tx.insert(doc) catch |err| return mapWriteError(err);
    return Result.OK;
}

pub export fn albedo_transaction_delete(tx: *Bucket.Transaction, queryBuffer: [*]u8, queryLen: u16) Result {
    var arena = std.heap.ArenaAllocator.init(ally);
    defer arena.deinit();
    const local_ally = arena.allocator();

    const docBufferProper = local_ally.dupe(u8, queryBuffer[0..queryLen]) catch {
        return Result.OutOfMemory;
    };

    var query = Query.parseRaw(local_ally, docBufferProper) catch |err| switch (err) {
        Query.QueryParsingErrors.OutOfMemory => return Result.OutOfMemory,
        else => return Result.Error,
    };
    defer query.deinit(local_ally);

    tx.delete(query) catch |err| return mapWriteError(err);
    return Result.OK;
}

pub export fn albedo_transaction_transform(
    tx: *Bucket.Transaction,
    queryBuffer: [*c]u8,
    iteratorOut: **Bucket.TransformIterator,
) Result {
    const queryLen = std.mem.readInt(u32, queryBuffer[0..4], .little);
    const queryBufProper = queryBuffer[0..queryLen];

    const arena_ptr = ally.create(std.heap.ArenaAllocator) catch return Result.OutOfMemory;
    arena_ptr.* = std.heap.ArenaAllocator.init(ally);
    var arena_owned_by_iterator = false;
    defer {
        if (!arena_owned_by_iterator) {
            arena_ptr.deinit();
            ally.destroy(arena_ptr);
        }
    }

    const query = Query.parseRaw(
        arena_ptr.allocator(),
        arena_ptr.allocator().dupe(u8, queryBufProper) catch return Result.OutOfMemory,
    ) catch |err| switch (err) {
        Query.QueryParsingErrors.OutOfMemory => return Result.OutOfMemory,
        else => return Result.Error,
    };

    const iter = tx.transformIterate(arena_ptr, query) catch |err| return mapWriteError(err);
    iter.owns_arena = true;
    arena_owned_by_iterator = true;
    iteratorOut.* = iter;
    return Result.OK;
}

pub export fn albedo_transaction_commit(tx: *Bucket.Transaction) Result {
    tx.commit() catch |err| return mapTransactionError(err);
    return Result.OK;
}

pub export fn albedo_transaction_rollback(tx: *Bucket.Transaction) Result {
    tx.rollback() catch |err| return mapTransactionError(err);
    return Result.OK;
}

pub export fn albedo_transaction_close(tx: *Bucket.Transaction) Result {
    tx.close() catch |err| return mapTransactionError(err);
    return Result.OK;
}

pub export fn albedo_ensure_index(bucket: *albedo.Bucket, path: [*:0]const u8, options_byte: u8) Result {
    const path_proper = std.mem.span(path);
    const index_options = IndexOptions{
        .unique = @intCast(options_byte & 0x01),
        .sparse = @intCast((options_byte >> 1) & 0x01),
        .reverse = @intCast((options_byte >> 2) & 0x01),
        .reserved = @intCast((options_byte >> 3) & 0x1F),
    };

    bucket.ensureIndex(path_proper, index_options) catch |err| switch (err) {
        error.OutOfMemory => {
            return Result.OutOfMemory;
        },
        error.TransactionActive => {
            return Result.TransactionActive;
        },
        else => {
            // std.debug.print("Failed to ensure index for path {s}, {any}\n", .{ path_proper, err });
            return Result.Error;
        },
    };

    return Result.OK;
}

pub export fn albedo_drop_index(bucket: *albedo.Bucket, path: [*:0]const u8) Result {
    const path_proper = std.mem.span(path);

    bucket.dropIndex(path_proper) catch |err| switch (err) {
        error.IndexNotFound => {
            return Result.NotFound;
        },
        error.OutOfMemory => {
            return Result.OutOfMemory;
        },
        error.TransactionActive => {
            return Result.TransactionActive;
        },
        else => {
            // std.debug.print("Failed to drop index for path {s}, {any}\n", .{ path_proper, err });
            return Result.Error;
        },
    };

    return Result.OK;
}

pub export fn albedo_list_indexes(bucket: *albedo.Bucket, outDoc: *[*c]u8) Result {
    var index_info = bucket.listIndexes() catch {
        return Result.Error;
    };
    defer index_info.deinit();

    var indexes_doc = bson.BSONDocument.initEmpty();
    var indexes_owned = false;
    defer if (indexes_owned) indexes_doc.deinit(ally);

    for (index_info.indexes) |entry| {
        const options = entry.value.options;

        // Build per-index options subdocument via formatter
        var options_doc = bson.fmt.serialize(.{
            .unique = options.unique == 1,
            .sparse = options.sparse == 1,
            .reverse = options.reverse == 1,
        }, ally) catch |err| switch (err) {
            error.OutOfMemory => return Result.OutOfMemory,
            else => return Result.Error,
        };
        defer options_doc.deinit(ally);

        // Attach options_doc under indexes.<path>
        const index_value = bson.BSONValue{ .document = options_doc };
        const next_indexes = indexes_doc.set(ally, entry.key, index_value) catch {
            return Result.OutOfMemory;
        };

        if (indexes_owned) {
            indexes_doc.deinit(ally);
        } else {
            indexes_owned = true;
        }
        indexes_doc = next_indexes;

        // options_doc is freed via defer; set() copied its bytes
    }

    var root_doc = bson.BSONDocument.initEmpty();
    var root_owned = false;
    defer if (root_owned) root_doc.deinit(ally);

    const next_root = root_doc.set(ally, "indexes", bson.BSONValue{ .document = indexes_doc }) catch {
        return Result.OutOfMemory;
    };
    root_owned = true;
    root_doc = next_root;

    const out_buf = ally.alloc(u8, root_doc.buffer.len) catch {
        return Result.OutOfMemory;
    };
    @memcpy(out_buf, root_doc.buffer);
    outDoc.* = @ptrCast(out_buf.ptr);

    return Result.OK;
}

pub export fn albedo_delete(bucket: *albedo.Bucket, queryBuffer: [*]u8, queryLen: u16) Result {
    var arena = std.heap.ArenaAllocator.init(ally);
    defer arena.deinit();
    const local_ally = arena.allocator();

    const docBufferProper = local_ally.dupe(u8, queryBuffer[0..queryLen]) catch {
        return Result.OutOfMemory;
    };

    var query = Query.parseRaw(local_ally, docBufferProper) catch |err| switch (err) {
        Query.QueryParsingErrors.OutOfMemory => {
            return Result.OutOfMemory;
        },
        else => {
            return Result.Error;
        },
    };
    defer query.deinit(local_ally);

    bucket.delete(query) catch |err| return mapWriteError(err);

    return Result.OK;
}

const ListIterator = albedo.Bucket.ListIterator;

pub const ListHandle = struct {
    iterator: *ListIterator,
    arena: *std.heap.ArenaAllocator,
};

pub export fn albedo_list(bucket: *albedo.Bucket, queryBuffer: [*]u8, outIterator: **ListHandle) Result {
    const queryLen = std.mem.readInt(u32, queryBuffer[0..4], .little);
    const queryArena = ally.create(std.heap.ArenaAllocator) catch return Result.OutOfMemory;
    queryArena.* = std.heap.ArenaAllocator.init(ally);
    var arena_owned_by_handle = false;
    defer {
        if (!arena_owned_by_handle) {
            queryArena.deinit();
            ally.destroy(queryArena);
        }
    }

    const local_ally = queryArena.allocator();
    const queryBufferProper = local_ally.dupe(u8, queryBuffer[0..queryLen]) catch return Result.OutOfMemory;

    const query = Query.parseRaw(local_ally, queryBufferProper) catch |err| return mapQueryParseError(err);

    const iterator = bucket.listIterate(queryArena, query) catch |err| return mapListError(err);
    const listHandle = local_ally.create(ListHandle) catch return Result.OutOfMemory;
    listHandle.* = ListHandle{
        .iterator = iterator,
        .arena = queryArena,
    };
    arena_owned_by_handle = true;
    outIterator.* = listHandle;

    return Result.OK;
}

pub export fn albedo_data(handle: *ListHandle, outDoc: *[*]u8) Result {
    const doc = handle.iterator.next(handle.iterator) catch |err| switch (err) {
        else => {
            // std.debug.print("Failed to iterate, {any}", .{iterErr});
            return Result.Error;
        },
    } orelse {
        return Result.EOS;
    };

    outDoc.* = @constCast(doc.buffer.ptr);

    return Result.OK;
}

pub export fn albedo_list_cursor_export(handle: *ListHandle, outCursor: *[*]u8) Result {
    var cursor_doc = handle.iterator.exportCursor(ally) catch |err| return mapListError(err);
    defer cursor_doc.deinit(ally);

    const out_buf = ally.alloc(u8, cursor_doc.buffer.len) catch return Result.OutOfMemory;
    @memcpy(out_buf, cursor_doc.buffer);
    outCursor.* = out_buf.ptr;
    return Result.OK;
}

pub export fn albedo_close_iterator(iterator: *ListHandle) Result {
    const arena_ptr = iterator.arena;
    iterator.iterator.deinit() catch {};
    arena_ptr.deinit();
    ally.destroy(arena_ptr);

    return Result.OK;
}

pub export fn albedo_vacuum(bucket: *Bucket) Result {
    bucket.vacuum() catch |err| return mapTransactionError(err);
    return Result.OK;
}

pub export fn albedo_flush(bucket: *Bucket) Result {
    bucket.flush() catch |err| return mapTransactionError(err);
    return Result.OK;
}

pub export fn albedo_transform(
    bucket: *Bucket,
    queryBuffer: [*c]u8,
    iteratorOut: **Bucket.TransformIterator,
) Result {
    const queryLen = std.mem.readInt(u32, queryBuffer[0..4], .little);
    const queryBufProper = queryBuffer[0..queryLen];

    const arena_ptr = ally.create(std.heap.ArenaAllocator) catch {
        return Result.OutOfMemory;
    };
    arena_ptr.* = std.heap.ArenaAllocator.init(ally);
    var arena_owned_by_iterator = false;
    defer {
        if (!arena_owned_by_iterator) {
            arena_ptr.deinit();
            ally.destroy(arena_ptr);
        }
    }

    const query = Query.parseRaw(
        arena_ptr.allocator(),
        arena_ptr.allocator().dupe(u8, queryBufProper) catch {
            return Result.OutOfMemory;
        },
    ) catch |err| switch (err) {
        Query.QueryParsingErrors.OutOfMemory => {
            return Result.OutOfMemory;
        },
        else => {
            return Result.Error;
        },
    };

    // Create an arena for the iterator and mark it as owned

    const iter = bucket.transformIterate(arena_ptr, query) catch |err| return mapWriteError(err);

    // Mark the arena as owned by the iterator so it will be cleaned up in close()
    iter.owns_arena = true;
    arena_owned_by_iterator = true;
    iteratorOut.* = iter;

    return Result.OK;
}

pub export fn albedo_transform_data(
    iterator: *Bucket.TransformIterator,
    outDoc: *[*c]u8,
) Result {
    const doc = iterator.data() catch |err| switch (err) {
        else => {
            return Result.Error;
        },
    } orelse {
        return Result.EOS;
    };

    outDoc.* = @constCast(doc.buffer.ptr);
    return Result.OK;
}

pub export fn albedo_transform_apply(
    iterator: *Bucket.TransformIterator,
    transformBuffer: [*c]u8,
) Result {
    const doc = if (transformBuffer != null) blk: {
        const docSize = std.mem.readInt(u32, transformBuffer[0..4], .little);
        const transformBufProper = transformBuffer[0..docSize];
        break :blk &bson.BSONDocument.init(transformBufProper);
    } else null;

    iterator.transform(doc) catch |err| return mapWriteError(err);

    return Result.OK;
}

pub export fn albedo_transform_close(iterator: *Bucket.TransformIterator) Result {
    iterator.close() catch {
        return Result.Error;
    };
    return Result.OK;
}

pub export fn albedo_replication_cursor(bucket: *Bucket, out_cursor: *?*ReplicationCursorHandle) Result {
    const cursor = bucket.replicationCursor() catch |err| return mapReplicationError(err);
    const handle = createReplicationCursorHandle(cursor) catch return Result.OutOfMemory;
    out_cursor.* = handle;
    return Result.OK;
}

pub export fn albedo_replication_read(
    bucket: *Bucket,
    from: ?*ReplicationCursorHandle,
    max_bytes: usize,
    out_batch: *[*c]u8,
    out_size: *usize,
) Result {
    const from_handle = from orelse return Result.InvalidCursor;
    const maybe_batch = bucket.readReplicationBatch(from_handle.cursor, max_bytes, ally) catch |err| return mapReplicationError(err);
    if (maybe_batch) |batch| {
        out_batch.* = @ptrCast(batch.ptr);
        out_size.* = batch.len;
        return Result.HasData;
    }
    out_batch.* = null;
    out_size.* = 0;
    return Result.EOS;
}

pub export fn albedo_replication_apply(
    bucket: *Bucket,
    data: [*]const u8,
    data_size: usize,
    out_cursor: *?*ReplicationCursorHandle,
) Result {
    const cursor = bucket.applyReplicationBatch(data[0..data_size]) catch |err| return mapReplicationError(err);
    const handle = createReplicationCursorHandle(cursor) catch return Result.OutOfMemory;
    out_cursor.* = handle;
    return Result.OK;
}

pub export fn albedo_replication_cursor_close(cursor: ?*ReplicationCursorHandle) Result {
    const handle = cursor orelse return Result.Error;
    ally.destroy(handle);
    return Result.OK;
}

const SubscriptionHandle = struct {
    sub: *Bucket.Subscription,
};

pub export fn albedo_subscribe(
    bucket: *Bucket,
    queryBuffer: [*]u8,
    outHandle: **SubscriptionHandle,
) Result {
    const queryLen = std.mem.readInt(u32, queryBuffer[0..4], .little);
    const queryBufProper = queryBuffer[0..queryLen];

    var q = Query.parseRaw(ally, queryBufProper) catch |err| return mapQueryParseError(err);

    const sub = bucket.subscribe(q) catch |err| switch (err) {
        error.WalNotActive => return Result.Error,
        error.OutOfMemory => {
            q.deinit(ally);
            return Result.OutOfMemory;
        },
        error.OplogGap => {
            q.deinit(ally);
            return Result.Error;
        },
    };

    const handle = ally.create(SubscriptionHandle) catch {
        sub.deinit();
        return Result.OutOfMemory;
    };
    handle.* = .{ .sub = sub };
    outHandle.* = handle;
    return Result.OK;
}

/// Poll the subscription for new change events.
/// Returns ALBEDO_HAS_DATA with `*out_doc` pointing to a BSON document
/// `{batch: [...event]}` whose memory is owned by the subscription and
/// valid until the next poll or close call.
/// Returns ALBEDO_EOS when there are no new events, or ALBEDO_OPLOG_GAP
/// when the subscriber has fallen behind the ring buffer.
pub export fn albedo_subscribe_poll(
    handle: *SubscriptionHandle,
    out_doc: *[*]u8,
    max_events: u32,
) Result {
    const maybe_doc = handle.sub.poll(max_events) catch |err| switch (err) {
        error.OplogGap => return Result.OplogGap,
        error.WalNotActive => return Result.Error,
        error.OutOfMemory => return Result.OutOfMemory,
    };
    if (maybe_doc) |doc| {
        out_doc.* = @constCast(doc.buffer.ptr);
        return Result.HasData;
    }
    return Result.EOS;
}

/// Return the latest committed oplog sequence number.
pub export fn albedo_subscribe_seqno(handle: *SubscriptionHandle) u64 {
    return handle.sub.currentSeqno();
}

/// Close and free a subscription handle.
pub export fn albedo_subscribe_close(handle: *SubscriptionHandle) Result {
    handle.sub.deinit();
    ally.destroy(handle);
    return Result.OK;
}

pub export fn albedo_bitsize() u32 {
    return @sizeOf(usize) * 8;
}

pub export fn albedo_version() u32 {
    return 1;
}

pub export fn albedo_malloc(size: usize) [*c]u8 {
    const mem = ally.alloc(u8, size) catch return 0;
    return @ptrCast(mem.ptr);
}

pub export fn albedo_free(ptr: [*c]u8, size: usize) void {
    ally.free(ptr[0..size]);
}

fn makeTempPath(allocator: std.mem.Allocator, name: []const u8) ![]u8 {
    return std.fmt.allocPrint(allocator, "{s}.bucket", .{name});
}

test "lib API open insert list close" {
    const allocator = testing.allocator;
    const path = try makeTempPath(allocator, "lib-api-open-insert-list-close");
    defer allocator.free(path);
    platform.deleteFile(path) catch {};
    defer platform.deleteFile(path) catch {};

    const path_z = try allocator.dupeZ(u8, path);
    defer allocator.free(path_z);

    var bucket: *Bucket = undefined;
    try testing.expectEqual(Result.OK, albedo_open(path_z.ptr, &bucket));
    defer _ = albedo_close(bucket);

    var doc = try bson.BSONDocument.fromJSON(allocator,
        \\{
        \\  "name": "Alice",
        \\  "age": 30
        \\}
    );
    defer doc.deinit(allocator);
    try testing.expectEqual(Result.OK, albedo_insert(bucket, @constCast(doc.buffer.ptr)));

    var list_query = try bson.BSONDocument.fromJSON(allocator,
        \\{
        \\  "query": {
        \\    "name": "Alice"
        \\  }
        \\}
    );
    defer list_query.deinit(allocator);

    var handle: *ListHandle = undefined;
    try testing.expectEqual(Result.OK, albedo_list(bucket, @constCast(list_query.buffer.ptr), &handle));
    defer _ = albedo_close_iterator(handle);

    var raw_doc_ptr: [*]u8 = undefined;
    try testing.expectEqual(Result.OK, albedo_data(handle, &raw_doc_ptr));

    const raw_len = std.mem.readInt(u32, raw_doc_ptr[0..4], .little);
    const listed = bson.BSONDocument.init(raw_doc_ptr[0..raw_len]);
    const listed_name = listed.get("name") orelse return error.TestExpectedEqual;
    try testing.expectEqualStrings("Alice", listed_name.string.value);

    try testing.expectEqual(Result.EOS, albedo_data(handle, &raw_doc_ptr));
}

test "lib API delete removes matched docs" {
    const allocator = testing.allocator;
    const path = try makeTempPath(allocator, "lib-api-delete-removes-matched-docs");
    defer allocator.free(path);
    platform.deleteFile(path) catch {};
    defer platform.deleteFile(path) catch {};

    const path_z = try allocator.dupeZ(u8, path);
    defer allocator.free(path_z);

    var bucket: *Bucket = undefined;
    try testing.expectEqual(Result.OK, albedo_open(path_z.ptr, &bucket));
    defer _ = albedo_close(bucket);

    var doc = try bson.BSONDocument.fromJSON(allocator,
        \\{
        \\  "name": "Bob",
        \\  "age": 44
        \\}
    );
    defer doc.deinit(allocator);
    try testing.expectEqual(Result.OK, albedo_insert(bucket, @constCast(doc.buffer.ptr)));

    var delete_query = try bson.BSONDocument.fromJSON(allocator,
        \\{
        \\  "query": {
        \\    "name": "Bob"
        \\  }
        \\}
    );
    defer delete_query.deinit(allocator);
    try testing.expectEqual(
        Result.OK,
        albedo_delete(bucket, @constCast(delete_query.buffer.ptr), @intCast(delete_query.buffer.len)),
    );

    var list_query = try bson.BSONDocument.fromJSON(allocator,
        \\{
        \\  "query": {
        \\    "name": "Bob"
        \\  }
        \\}
    );
    defer list_query.deinit(allocator);

    var handle: *ListHandle = undefined;
    try testing.expectEqual(Result.OK, albedo_list(bucket, @constCast(list_query.buffer.ptr), &handle));
    defer _ = albedo_close_iterator(handle);

    var raw_doc_ptr: [*]u8 = undefined;
    try testing.expectEqual(Result.EOS, albedo_data(handle, &raw_doc_ptr));
}

test "lib API list_indexes returns index options" {
    const allocator = testing.allocator;
    const path = try makeTempPath(allocator, "lib-api-list-indexes-returns-index-options");
    defer allocator.free(path);
    platform.deleteFile(path) catch {};
    defer platform.deleteFile(path) catch {};

    const path_z = try allocator.dupeZ(u8, path);
    defer allocator.free(path_z);

    var bucket: *Bucket = undefined;
    try testing.expectEqual(Result.OK, albedo_open(path_z.ptr, &bucket));
    defer _ = albedo_close(bucket);

    // Create a sparse index on "name"
    const name_z = try allocator.dupeZ(u8, "name");
    defer allocator.free(name_z);
    try testing.expectEqual(Result.OK, albedo_ensure_index(bucket, name_z.ptr, 0x02));

    var raw_ptr: [*c]u8 = undefined;
    try testing.expectEqual(Result.OK, albedo_list_indexes(bucket, &raw_ptr));

    const raw_len = std.mem.readInt(u32, raw_ptr[0..4], .little);
    defer albedo_free(raw_ptr, raw_len);

    const doc = bson.BSONDocument.init(raw_ptr[0..raw_len]);
    const sparse_val = doc.getPath("indexes.name.sparse") orelse return error.TestExpectedEqual;
    try testing.expectEqual(true, sparse_val.boolean.value);
}

test "lib API transform updates matching doc" {
    const allocator = testing.allocator;
    const path = try makeTempPath(allocator, "lib-api-transform-updates-matching-doc");
    defer allocator.free(path);
    platform.deleteFile(path) catch {};
    defer platform.deleteFile(path) catch {};

    const path_z = try allocator.dupeZ(u8, path);
    defer allocator.free(path_z);

    var bucket: *Bucket = undefined;
    try testing.expectEqual(Result.OK, albedo_open(path_z.ptr, &bucket));
    defer _ = albedo_close(bucket);

    var doc = try bson.BSONDocument.fromJSON(allocator,
        \\{
        \\  "name": "Carol",
        \\  "age": 20
        \\}
    );
    defer doc.deinit(allocator);
    try testing.expectEqual(Result.OK, albedo_insert(bucket, @constCast(doc.buffer.ptr)));

    var transform_query = try bson.BSONDocument.fromJSON(allocator,
        \\{
        \\  "query": {
        \\    "name": "Carol"
        \\  }
        \\}
    );
    defer transform_query.deinit(allocator);

    var iterator: *Bucket.TransformIterator = undefined;
    try testing.expectEqual(Result.OK, albedo_transform(bucket, @constCast(transform_query.buffer.ptr), &iterator));

    var current_ptr: [*c]u8 = undefined;
    try testing.expectEqual(Result.OK, albedo_transform_data(iterator, &current_ptr));

    var updated = try bson.BSONDocument.fromJSON(allocator,
        \\{
        \\  "name": "Carol",
        \\  "age": 21
        \\}
    );
    defer updated.deinit(allocator);
    try testing.expectEqual(Result.OK, albedo_transform_apply(iterator, @constCast(updated.buffer.ptr)));
    try testing.expectEqual(Result.EOS, albedo_transform_data(iterator, &current_ptr));
    try testing.expectEqual(Result.OK, albedo_transform_close(iterator));

    var list_query = try bson.BSONDocument.fromJSON(allocator,
        \\{
        \\  "query": {
        \\    "name": "Carol"
        \\  }
        \\}
    );
    defer list_query.deinit(allocator);

    var handle: *ListHandle = undefined;
    try testing.expectEqual(Result.OK, albedo_list(bucket, @constCast(list_query.buffer.ptr), &handle));
    defer _ = albedo_close_iterator(handle);

    var listed_ptr: [*]u8 = undefined;
    try testing.expectEqual(Result.OK, albedo_data(handle, &listed_ptr));
    const listed_len = std.mem.readInt(u32, listed_ptr[0..4], .little);
    const listed = bson.BSONDocument.init(listed_ptr[0..listed_len]);
    const name = listed.get("name") orelse return error.TestExpectedEqual;
    try testing.expectEqualStrings("Carol", name.string.value);
    const age = listed.get("age") orelse return error.TestExpectedEqual;
    const age_num = switch (age) {
        .int32 => |v| @as(i64, v.value),
        .int64 => |v| v.value,
        else => return error.TestExpectedEqual,
    };
    try testing.expectEqual(@as(i64, 21), age_num);
}

test "lib API transaction commit lifecycle" {
    const allocator = testing.allocator;
    const path = try makeTempPath(allocator, "lib-api-transaction-commit");
    defer allocator.free(path);
    platform.deleteFile(path) catch {};
    defer platform.deleteFile(path) catch {};

    const wal_path = try std.fmt.allocPrint(allocator, "{s}-wal", .{path});
    defer allocator.free(wal_path);
    std.fs.cwd().deleteFile(wal_path) catch {};
    defer std.fs.cwd().deleteFile(wal_path) catch {};

    const shm_path = try std.fmt.allocPrint(allocator, "{s}-wal-shm", .{path});
    defer allocator.free(shm_path);
    std.fs.cwd().deleteFile(shm_path) catch {};
    defer std.fs.cwd().deleteFile(shm_path) catch {};

    const path_z = try allocator.dupeZ(u8, path);
    defer allocator.free(path_z);

    var writer: *Bucket = undefined;
    try testing.expectEqual(Result.OK, albedo_open(path_z.ptr, &writer));
    defer _ = albedo_close(writer);

    var reader: *Bucket = undefined;
    try testing.expectEqual(Result.OK, albedo_open(path_z.ptr, &reader));
    defer _ = albedo_close(reader);

    var tx: *Bucket.Transaction = undefined;
    try testing.expectEqual(Result.OK, albedo_transaction_begin(writer, &tx));
    var tx_closed = false;
    defer {
        if (!tx_closed) _ = albedo_transaction_close(tx);
    }

    var staged = try bson.BSONDocument.fromJSON(allocator, "{\"name\":\"committed\"}");
    defer staged.deinit(allocator);
    try testing.expectEqual(Result.OK, albedo_transaction_insert(tx, @constCast(staged.buffer.ptr)));

    var list_query = try bson.BSONDocument.fromJSON(allocator,
        \\{
        \\  "query": {
        \\    "name": "committed"
        \\  }
        \\}
    );
    defer list_query.deinit(allocator);

    var before_handle: *ListHandle = undefined;
    try testing.expectEqual(Result.OK, albedo_list(reader, @constCast(list_query.buffer.ptr), &before_handle));
    defer _ = albedo_close_iterator(before_handle);
    var raw_doc_ptr: [*]u8 = undefined;
    try testing.expectEqual(Result.EOS, albedo_data(before_handle, &raw_doc_ptr));

    try testing.expectEqual(Result.OK, albedo_transaction_commit(tx));
    try testing.expectEqual(Result.OK, albedo_transaction_close(tx));
    tx_closed = true;

    var after_handle: *ListHandle = undefined;
    try testing.expectEqual(Result.OK, albedo_list(reader, @constCast(list_query.buffer.ptr), &after_handle));
    defer _ = albedo_close_iterator(after_handle);
    try testing.expectEqual(Result.OK, albedo_data(after_handle, &raw_doc_ptr));
}

test "lib API transaction rollback transform" {
    const allocator = testing.allocator;
    const path = try makeTempPath(allocator, "lib-api-transaction-rollback-transform");
    defer allocator.free(path);
    platform.deleteFile(path) catch {};
    defer platform.deleteFile(path) catch {};

    const wal_path = try std.fmt.allocPrint(allocator, "{s}-wal", .{path});
    defer allocator.free(wal_path);
    std.fs.cwd().deleteFile(wal_path) catch {};
    defer std.fs.cwd().deleteFile(wal_path) catch {};

    const shm_path = try std.fmt.allocPrint(allocator, "{s}-wal-shm", .{path});
    defer allocator.free(shm_path);
    std.fs.cwd().deleteFile(shm_path) catch {};
    defer std.fs.cwd().deleteFile(shm_path) catch {};

    const path_z = try allocator.dupeZ(u8, path);
    defer allocator.free(path_z);

    var bucket: *Bucket = undefined;
    try testing.expectEqual(Result.OK, albedo_open(path_z.ptr, &bucket));
    defer _ = albedo_close(bucket);

    var doc = try bson.BSONDocument.fromJSON(allocator, "{\"name\":\"Alice\"}");
    defer doc.deinit(allocator);
    try testing.expectEqual(Result.OK, albedo_insert(bucket, @constCast(doc.buffer.ptr)));

    var tx: *Bucket.Transaction = undefined;
    try testing.expectEqual(Result.OK, albedo_transaction_begin(bucket, &tx));
    var tx_closed = false;
    defer {
        if (!tx_closed) _ = albedo_transaction_close(tx);
    }

    var transform_query = try bson.BSONDocument.fromJSON(allocator,
        \\{
        \\  "query": {
        \\    "name": "Alice"
        \\  }
        \\}
    );
    defer transform_query.deinit(allocator);

    var iterator: *Bucket.TransformIterator = undefined;
    try testing.expectEqual(Result.OK, albedo_transaction_transform(tx, @constCast(transform_query.buffer.ptr), &iterator));

    var current_ptr: [*c]u8 = undefined;
    try testing.expectEqual(Result.OK, albedo_transform_data(iterator, &current_ptr));

    var updated = try bson.BSONDocument.fromJSON(allocator, "{\"name\":\"Alicia\"}");
    defer updated.deinit(allocator);
    try testing.expectEqual(Result.OK, albedo_transform_apply(iterator, @constCast(updated.buffer.ptr)));
    try testing.expectEqual(Result.OK, albedo_transform_close(iterator));

    try testing.expectEqual(Result.OK, albedo_transaction_rollback(tx));
    try testing.expectEqual(Result.OK, albedo_transaction_close(tx));
    tx_closed = true;

    var alice_query = try bson.BSONDocument.fromJSON(allocator, "{\"query\":{\"name\":\"Alice\"}}");
    defer alice_query.deinit(allocator);
    var alice_handle: *ListHandle = undefined;
    try testing.expectEqual(Result.OK, albedo_list(bucket, @constCast(alice_query.buffer.ptr), &alice_handle));
    defer _ = albedo_close_iterator(alice_handle);
    var listed_ptr: [*]u8 = undefined;
    try testing.expectEqual(Result.OK, albedo_data(alice_handle, &listed_ptr));

    var alicia_query = try bson.BSONDocument.fromJSON(allocator, "{\"query\":{\"name\":\"Alicia\"}}");
    defer alicia_query.deinit(allocator);
    var alicia_handle: *ListHandle = undefined;
    try testing.expectEqual(Result.OK, albedo_list(bucket, @constCast(alicia_query.buffer.ptr), &alicia_handle));
    defer _ = albedo_close_iterator(alicia_handle);
    try testing.expectEqual(Result.EOS, albedo_data(alicia_handle, &listed_ptr));
}

test "lib API transaction guardrails" {
    const allocator = testing.allocator;
    const path = try makeTempPath(allocator, "lib-api-transaction-guardrails");
    defer allocator.free(path);
    platform.deleteFile(path) catch {};
    defer platform.deleteFile(path) catch {};

    const wal_path = try std.fmt.allocPrint(allocator, "{s}-wal", .{path});
    defer allocator.free(wal_path);
    std.fs.cwd().deleteFile(wal_path) catch {};
    defer std.fs.cwd().deleteFile(wal_path) catch {};

    const shm_path = try std.fmt.allocPrint(allocator, "{s}-wal-shm", .{path});
    defer allocator.free(shm_path);
    std.fs.cwd().deleteFile(shm_path) catch {};
    defer std.fs.cwd().deleteFile(shm_path) catch {};

    const path_z = try allocator.dupeZ(u8, path);
    defer allocator.free(path_z);

    var bucket: *Bucket = undefined;
    try testing.expectEqual(Result.OK, albedo_open(path_z.ptr, &bucket));
    defer _ = albedo_close(bucket);

    var doc = try bson.BSONDocument.fromJSON(allocator, "{\"name\":\"guard\"}");
    defer doc.deinit(allocator);
    try testing.expectEqual(Result.OK, albedo_insert(bucket, @constCast(doc.buffer.ptr)));

    var tx: *Bucket.Transaction = undefined;
    try testing.expectEqual(Result.OK, albedo_transaction_begin(bucket, &tx));
    defer _ = albedo_transaction_close(tx);

    var second_tx: *Bucket.Transaction = undefined;
    try testing.expectEqual(Result.TransactionActive, albedo_transaction_begin(bucket, &second_tx));

    var blocked = try bson.BSONDocument.fromJSON(allocator, "{\"name\":\"blocked\"}");
    defer blocked.deinit(allocator);
    try testing.expectEqual(Result.TransactionActive, albedo_insert(bucket, @constCast(blocked.buffer.ptr)));

    var transform_query = try bson.BSONDocument.fromJSON(allocator, "{\"query\":{\"name\":\"guard\"}}");
    defer transform_query.deinit(allocator);

    var iterator: *Bucket.TransformIterator = undefined;
    try testing.expectEqual(Result.OK, albedo_transaction_transform(tx, @constCast(transform_query.buffer.ptr), &iterator));
    defer _ = albedo_transform_close(iterator);

    try testing.expectEqual(Result.TransactionBusy, albedo_transaction_commit(tx));
    try testing.expectEqual(Result.TransactionBusy, albedo_transaction_rollback(tx));
}

test "lib API returns errors for invalid query payloads" {
    const allocator = testing.allocator;
    const path = try makeTempPath(allocator, "lib-api-invalid-query-payloads");
    defer allocator.free(path);
    platform.deleteFile(path) catch {};
    defer platform.deleteFile(path) catch {};

    const path_z = try allocator.dupeZ(u8, path);
    defer allocator.free(path_z);

    var bucket: *Bucket = undefined;
    try testing.expectEqual(Result.OK, albedo_open(path_z.ptr, &bucket));
    defer _ = albedo_close(bucket);

    var bad_query = try bson.BSONDocument.fromJSON(allocator,
        \\{
        \\  "query": {
        \\    "name": {
        \\      "$nope": "Alice"
        \\    }
        \\  }
        \\}
    );
    defer bad_query.deinit(allocator);

    var handle: *ListHandle = undefined;
    try testing.expectEqual(Result.Error, albedo_list(bucket, @constCast(bad_query.buffer.ptr), &handle));
    try testing.expectEqual(
        Result.Error,
        albedo_delete(bucket, @constCast(bad_query.buffer.ptr), @intCast(bad_query.buffer.len)),
    );

    var transform_iterator: *Bucket.TransformIterator = undefined;
    try testing.expectEqual(
        Result.Error,
        albedo_transform(bucket, @constCast(bad_query.buffer.ptr), &transform_iterator),
    );
}

test "lib API list cursor export resumes stream" {
    const allocator = testing.allocator;
    const path = try makeTempPath(allocator, "lib-api-list-cursor-export");
    defer allocator.free(path);
    platform.deleteFile(path) catch {};
    defer platform.deleteFile(path) catch {};

    const path_z = try allocator.dupeZ(u8, path);
    defer allocator.free(path_z);

    var bucket: *Bucket = undefined;
    try testing.expectEqual(Result.OK, albedo_open(path_z.ptr, &bucket));
    defer _ = albedo_close(bucket);

    for ([_][]const u8{ "A", "B", "C" }) |name| {
        const json = try std.fmt.allocPrint(allocator, "{{\"name\":\"{s}\"}}", .{name});
        defer allocator.free(json);
        var doc = try bson.BSONDocument.fromJSON(allocator, json);
        defer doc.deinit(allocator);
        try testing.expectEqual(Result.OK, albedo_insert(bucket, @constCast(doc.buffer.ptr)));
    }

    var list_query = try bson.BSONDocument.fromJSON(allocator, "{}");
    defer list_query.deinit(allocator);

    var handle: *ListHandle = undefined;
    try testing.expectEqual(Result.OK, albedo_list(bucket, @constCast(list_query.buffer.ptr), &handle));
    defer _ = albedo_close_iterator(handle);

    var raw_doc_ptr: [*]u8 = undefined;
    try testing.expectEqual(Result.OK, albedo_data(handle, &raw_doc_ptr));

    var raw_cursor_ptr: [*]u8 = undefined;
    try testing.expectEqual(Result.OK, albedo_list_cursor_export(handle, &raw_cursor_ptr));
    const raw_cursor_len = std.mem.readInt(u32, raw_cursor_ptr[0..4], .little);
    defer albedo_free(raw_cursor_ptr, raw_cursor_len);

    const cursor_doc = bson.BSONDocument.init(raw_cursor_ptr[0..raw_cursor_len]);
    var resume_query = try list_query.set(allocator, "cursor", bson.BSONValue.init(cursor_doc));
    defer resume_query.deinit(allocator);

    var resumed_handle: *ListHandle = undefined;
    try testing.expectEqual(Result.OK, albedo_list(bucket, @constCast(resume_query.buffer.ptr), &resumed_handle));
    defer _ = albedo_close_iterator(resumed_handle);

    var resumed_doc_ptr: [*]u8 = undefined;
    try testing.expectEqual(Result.OK, albedo_data(resumed_handle, &resumed_doc_ptr));
    const resumed_len = std.mem.readInt(u32, resumed_doc_ptr[0..4], .little);
    const resumed_doc = bson.BSONDocument.init(resumed_doc_ptr[0..resumed_len]);
    try testing.expectEqualStrings("B", resumed_doc.get("name").?.string.value);
}

test "lib API list rejects invalid cursor" {
    const allocator = testing.allocator;
    const path = try makeTempPath(allocator, "lib-api-invalid-cursor");
    defer allocator.free(path);
    platform.deleteFile(path) catch {};
    defer platform.deleteFile(path) catch {};

    const path_z = try allocator.dupeZ(u8, path);
    defer allocator.free(path_z);

    var bucket: *Bucket = undefined;
    try testing.expectEqual(Result.OK, albedo_open(path_z.ptr, &bucket));
    defer _ = albedo_close(bucket);

    var query_doc = try bson.BSONDocument.fromJSON(allocator,
        \\{
        \\  "cursor": {
        \\    "version": 2,
        \\    "mode": "full_scan"
        \\  }
        \\}
    );
    defer query_doc.deinit(allocator);

    var handle: *ListHandle = undefined;
    try testing.expectEqual(Result.InvalidCursor, albedo_list(bucket, @constCast(query_doc.buffer.ptr), &handle));
}

test "lib API list rejects unsupported cursor query" {
    const allocator = testing.allocator;
    const path = try makeTempPath(allocator, "lib-api-unsupported-cursor-query");
    defer allocator.free(path);
    platform.deleteFile(path) catch {};
    defer platform.deleteFile(path) catch {};

    const path_z = try allocator.dupeZ(u8, path);
    defer allocator.free(path_z);

    var bucket: *Bucket = undefined;
    try testing.expectEqual(Result.OK, albedo_open(path_z.ptr, &bucket));
    defer _ = albedo_close(bucket);

    var query_doc = try bson.BSONDocument.fromJSON(allocator,
        \\{
        \\  "sort": {
        \\    "asc": "name"
        \\  },
        \\  "cursor": {
        \\    "version": 1,
        \\    "mode": "full_scan"
        \\  }
        \\}
    );
    defer query_doc.deinit(allocator);

    var handle: *ListHandle = undefined;
    try testing.expectEqual(Result.UnsupportedCursorQuery, albedo_list(bucket, @constCast(query_doc.buffer.ptr), &handle));
}

test "lib API insert reports duplicate key" {
    const allocator = testing.allocator;
    const path = try makeTempPath(allocator, "lib-api-insert-duplicate-key");
    defer allocator.free(path);
    platform.deleteFile(path) catch {};
    defer platform.deleteFile(path) catch {};

    const path_z = try allocator.dupeZ(u8, path);
    defer allocator.free(path_z);

    var bucket: *Bucket = undefined;
    try testing.expectEqual(Result.OK, albedo_open(path_z.ptr, &bucket));
    defer _ = albedo_close(bucket);

    // unique bit is options byte bit 0
    try testing.expectEqual(Result.OK, albedo_ensure_index(bucket, "email", 0x01));

    var first = try bson.BSONDocument.fromJSON(allocator,
        \\{
        \\  "email": "same@example.com",
        \\  "name": "First"
        \\}
    );
    defer first.deinit(allocator);
    try testing.expectEqual(Result.OK, albedo_insert(bucket, @constCast(first.buffer.ptr)));

    var duplicate = try bson.BSONDocument.fromJSON(allocator,
        \\{
        \\  "email": "same@example.com",
        \\  "name": "Second"
        \\}
    );
    defer duplicate.deinit(allocator);
    try testing.expectEqual(Result.DuplicateKey, albedo_insert(bucket, @constCast(duplicate.buffer.ptr)));
}

test "lib API default _id index is unique" {
    const allocator = testing.allocator;
    const path = try makeTempPath(allocator, "lib-api-default-id-index-unique");
    defer allocator.free(path);
    platform.deleteFile(path) catch {};
    defer platform.deleteFile(path) catch {};

    const path_z = try allocator.dupeZ(u8, path);
    defer allocator.free(path_z);

    var bucket: *Bucket = undefined;
    try testing.expectEqual(Result.OK, albedo_open(path_z.ptr, &bucket));
    defer _ = albedo_close(bucket);

    var first = try bson.BSONDocument.fromJSON(allocator,
        \\{
        \\  "_id": "same-id",
        \\  "name": "First"
        \\}
    );
    defer first.deinit(allocator);
    try testing.expectEqual(Result.OK, albedo_insert(bucket, @constCast(first.buffer.ptr)));

    var duplicate = try bson.BSONDocument.fromJSON(allocator,
        \\{
        \\  "_id": "same-id",
        \\  "name": "Second"
        \\}
    );
    defer duplicate.deinit(allocator);
    try testing.expectEqual(Result.DuplicateKey, albedo_insert(bucket, @constCast(duplicate.buffer.ptr)));
}

test "lib API drop index returns not found for missing path" {
    const allocator = testing.allocator;
    const path = try makeTempPath(allocator, "lib-api-drop-index-not-found");
    defer allocator.free(path);
    platform.deleteFile(path) catch {};
    defer platform.deleteFile(path) catch {};

    const path_z = try allocator.dupeZ(u8, path);
    defer allocator.free(path_z);

    var bucket: *Bucket = undefined;
    try testing.expectEqual(Result.OK, albedo_open(path_z.ptr, &bucket));
    defer _ = albedo_close(bucket);

    try testing.expectEqual(Result.NotFound, albedo_drop_index(bucket, "does.not.exist"));
}

test "lib API replication_apply rejects invalid payload size" {
    const allocator = testing.allocator;
    const path = try makeTempPath(allocator, "lib-api-replication-apply-invalid-size");
    defer allocator.free(path);
    platform.deleteFile(path) catch {};
    defer platform.deleteFile(path) catch {};

    const path_z = try allocator.dupeZ(u8, path);
    defer allocator.free(path_z);

    var bucket: *Bucket = undefined;
    try testing.expectEqual(Result.OK, albedo_open(path_z.ptr, &bucket));
    defer _ = albedo_close(bucket);

    const bad = [_]u8{ 1, 2, 3, 4 };
    var cursor: ?*ReplicationCursorHandle = null;
    try testing.expectEqual(Result.InvalidFormat, albedo_replication_apply(bucket, bad[0..].ptr, bad.len, &cursor));
}

test "lib API replication read/apply smoke test" {
    const allocator = testing.allocator;

    const primary_path = try makeTempPath(allocator, "lib-api-replication-primary");
    defer allocator.free(primary_path);
    platform.deleteFile(primary_path) catch {};
    defer platform.deleteFile(primary_path) catch {};

    const replica_path = try makeTempPath(allocator, "lib-api-replication-replica");
    defer allocator.free(replica_path);
    platform.deleteFile(replica_path) catch {};
    defer platform.deleteFile(replica_path) catch {};

    const primary_path_z = try allocator.dupeZ(u8, primary_path);
    defer allocator.free(primary_path_z);
    const replica_path_z = try allocator.dupeZ(u8, replica_path);
    defer allocator.free(replica_path_z);

    var primary: *Bucket = undefined;
    var replica: *Bucket = undefined;
    try testing.expectEqual(Result.OK, albedo_open(primary_path_z.ptr, &primary));
    defer _ = albedo_close(primary);
    try testing.expectEqual(Result.OK, albedo_open(replica_path_z.ptr, &replica));
    defer _ = albedo_close(replica);

    var initial_cursor: ?*ReplicationCursorHandle = null;
    try testing.expectEqual(Result.OK, albedo_replication_cursor(primary, &initial_cursor));
    defer if (initial_cursor) |cursor| {
        testing.expectEqual(Result.OK, albedo_replication_cursor_close(cursor)) catch unreachable;
    };

    var doc = try bson.BSONDocument.fromJSON(allocator,
        \\{
        \\  "name": "replicated"
        \\}
    );
    defer doc.deinit(allocator);
    try testing.expectEqual(Result.OK, albedo_insert(primary, @constCast(doc.buffer.ptr)));
    try testing.expectEqual(Result.OK, albedo_flush(primary));

    var batch_ptr: [*c]u8 = undefined;
    var batch_size: usize = 0;
    try testing.expectEqual(Result.HasData, albedo_replication_read(primary, initial_cursor, 0, &batch_ptr, &batch_size));
    try testing.expect(batch_size > 0);

    var replica_cursor: ?*ReplicationCursorHandle = null;
    try testing.expectEqual(Result.OK, albedo_replication_apply(replica, @ptrCast(batch_ptr), batch_size, &replica_cursor));
    defer if (replica_cursor) |cursor| {
        testing.expectEqual(Result.OK, albedo_replication_cursor_close(cursor)) catch unreachable;
    };
    albedo_free(batch_ptr, batch_size);

    var primary_cursor: ?*ReplicationCursorHandle = null;
    try testing.expectEqual(Result.OK, albedo_replication_cursor(primary, &primary_cursor));
    defer if (primary_cursor) |cursor| {
        testing.expectEqual(Result.OK, albedo_replication_cursor_close(cursor)) catch unreachable;
    };
    try testing.expectEqual(primary_cursor.?.cursor.generation, replica_cursor.?.cursor.generation);
    try testing.expectEqual(primary_cursor.?.cursor.next_frame_index, replica_cursor.?.cursor.next_frame_index);

    try testing.expectEqual(Result.EOS, albedo_replication_read(primary, replica_cursor, 0, &batch_ptr, &batch_size));
}

test "lib API open_with_options" {
    const allocator = testing.allocator;

    // — defaults (empty options) —
    {
        const path = try makeTempPath(allocator, "lib-api-opts-defaults");
        defer allocator.free(path);
        platform.deleteFile(path) catch {};
        defer platform.deleteFile(path) catch {};

        const path_z = try allocator.dupeZ(u8, path);
        defer allocator.free(path_z);

        var opts_doc = try bson.BSONDocument.fromJSON(allocator, "{}");
        defer opts_doc.deinit(allocator);

        var bucket: *Bucket = undefined;
        try testing.expectEqual(Result.OK, albedo_open_with_options(path_z.ptr, @constCast(opts_doc.buffer.ptr), &bucket));
        defer _ = albedo_close(bucket);

        try testing.expectEqual(true, bucket.autoVaccuum);
        try testing.expectEqual(albedo.WriteDurability{ .periodic = 100 }, bucket.write_durability);
        try testing.expectEqual(albedo.ReadDurability.shared, bucket.read_durability);
    }

    // — all options set, insert + query —
    {
        const path = try makeTempPath(allocator, "lib-api-opts-full");
        defer allocator.free(path);
        platform.deleteFile(path) catch {};
        defer platform.deleteFile(path) catch {};

        const path_z = try allocator.dupeZ(u8, path);
        defer allocator.free(path_z);

        var opts_doc = try bson.BSONDocument.fromJSON(allocator,
            \\{
            \\  "auto_vaccuum": false,
            \\  "write_durability": { "periodic": 200 },
            \\  "read_durability": "process"
            \\}
        );
        defer opts_doc.deinit(allocator);

        var bucket: *Bucket = undefined;
        try testing.expectEqual(Result.OK, albedo_open_with_options(path_z.ptr, @constCast(opts_doc.buffer.ptr), &bucket));
        defer _ = albedo_close(bucket);

        try testing.expectEqual(false, bucket.autoVaccuum);
        try testing.expectEqual(albedo.WriteDurability{ .periodic = 200 }, bucket.write_durability);
        try testing.expectEqual(albedo.ReadDurability.process, bucket.read_durability);

        // insert and query back
        var doc = try bson.BSONDocument.fromJSON(allocator,
            \\{ "name": "Dave", "age": 25 }
        );
        defer doc.deinit(allocator);
        try testing.expectEqual(Result.OK, albedo_insert(bucket, @constCast(doc.buffer.ptr)));

        var list_query = try bson.BSONDocument.fromJSON(allocator,
            \\{ "query": { "name": "Dave" } }
        );
        defer list_query.deinit(allocator);

        var handle: *ListHandle = undefined;
        try testing.expectEqual(Result.OK, albedo_list(bucket, @constCast(list_query.buffer.ptr), &handle));
        defer _ = albedo_close_iterator(handle);

        var raw_doc_ptr: [*]u8 = undefined;
        try testing.expectEqual(Result.OK, albedo_data(handle, &raw_doc_ptr));

        const raw_len = std.mem.readInt(u32, raw_doc_ptr[0..4], .little);
        const listed = bson.BSONDocument.init(raw_doc_ptr[0..raw_len]);
        const listed_name = listed.get("name") orelse return error.TestExpectedEqual;
        try testing.expectEqualStrings("Dave", listed_name.string.value);

        try testing.expectEqual(Result.EOS, albedo_data(handle, &raw_doc_ptr));
    }
}

test "bson fmt tagged union roundtrip" {
    const allocator = testing.allocator;
    const Mode = albedo.WriteDurability;

    // void variant: "all"
    {
        const T = struct { mode: Mode };
        var doc = try bson.fmt.serialize(T{ .mode = .all }, allocator);
        defer doc.deinit(allocator);
        var parsed = try bson.fmt.parse(T, doc, allocator);
        defer parsed.deinit();
        try testing.expectEqual(Mode.all, parsed.value.mode);
    }

    // void variant: "manual"
    {
        const T = struct { mode: Mode };
        var doc = try bson.fmt.serialize(T{ .mode = .manual }, allocator);
        defer doc.deinit(allocator);
        var parsed = try bson.fmt.parse(T, doc, allocator);
        defer parsed.deinit();
        try testing.expectEqual(Mode.manual, parsed.value.mode);
    }

    // payload variant: { "periodic": 50 }
    {
        const T = struct { mode: Mode };
        var doc = try bson.fmt.serialize(T{ .mode = .{ .periodic = 50 } }, allocator);
        defer doc.deinit(allocator);
        var parsed = try bson.fmt.parse(T, doc, allocator);
        defer parsed.deinit();
        try testing.expectEqual(Mode{ .periodic = 50 }, parsed.value.mode);
    }
}

// ── Memory-leak tests ────────────────────────────────────────────────────────
//
// std.testing.allocator is a GeneralPurposeAllocator that fails the test if
// any allocation that was created through `ally` is still live when the test
// exits.  Each test below exercises a distinct cleanup path so that a missing
// deinit/destroy/free anywhere in the call chain is caught automatically.

test "lib API subscription lifecycle does not leak" {
    const allocator = testing.allocator;
    const path = try makeTempPath(allocator, "lib-api-sub-lifecycle");
    defer allocator.free(path);
    platform.deleteFile(path) catch {};
    defer platform.deleteFile(path) catch {};

    const wal_path = try std.fmt.allocPrint(allocator, "{s}-wal", .{path});
    defer allocator.free(wal_path);
    std.fs.cwd().deleteFile(wal_path) catch {};
    defer std.fs.cwd().deleteFile(wal_path) catch {};

    const shm_path = try std.fmt.allocPrint(allocator, "{s}-wal-shm", .{path});
    defer allocator.free(shm_path);
    std.fs.cwd().deleteFile(shm_path) catch {};
    defer std.fs.cwd().deleteFile(shm_path) catch {};

    const path_z = try allocator.dupeZ(u8, path);
    defer allocator.free(path_z);

    var bucket: *Bucket = undefined;
    try testing.expectEqual(Result.OK, albedo_open(path_z.ptr, &bucket));
    defer _ = albedo_close(bucket);

    // Subscribe before inserting so only future events are visible.
    var sub_query = try bson.BSONDocument.fromJSON(allocator, "{}");
    defer sub_query.deinit(allocator);

    var sub_handle: *SubscriptionHandle = undefined;
    try testing.expectEqual(Result.OK, albedo_subscribe(bucket, @constCast(sub_query.buffer.ptr), &sub_handle));

    // Insert a document AFTER subscribing so the subscription sees it.
    var doc = try bson.BSONDocument.fromJSON(allocator, "{\"name\":\"Eve\"}");
    defer doc.deinit(allocator);
    try testing.expectEqual(Result.OK, albedo_insert(bucket, @constCast(doc.buffer.ptr)));

    // Poll — must see a BSON batch with at least one event.
    var batch_ptr: [*]u8 = undefined;
    try testing.expectEqual(Result.HasData, albedo_subscribe_poll(sub_handle, &batch_ptr, 64));
    const batch_len = std.mem.readInt(u32, batch_ptr[0..4], .little);
    const batch_doc = bson.BSONDocument.init(batch_ptr[0..batch_len]);
    const batch_arr = batch_doc.get("batch") orelse return error.TestExpectedEqual;
    try testing.expect(batch_arr.array.keyNumber() >= 1);

    // A second poll with no new writes must return EOS.
    try testing.expectEqual(Result.EOS, albedo_subscribe_poll(sub_handle, &batch_ptr, 64));

    // Close frees SubscriptionHandle + internal Subscription + stored Query.
    try testing.expectEqual(Result.OK, albedo_subscribe_close(sub_handle));
}

test "lib API subscription idle seqno does not leak" {
    const allocator = testing.allocator;
    const path = try makeTempPath(allocator, "lib-api-sub-seqno");
    defer allocator.free(path);
    platform.deleteFile(path) catch {};
    defer platform.deleteFile(path) catch {};

    const wal_path = try std.fmt.allocPrint(allocator, "{s}-wal", .{path});
    defer allocator.free(wal_path);
    std.fs.cwd().deleteFile(wal_path) catch {};
    defer std.fs.cwd().deleteFile(wal_path) catch {};

    const shm_path = try std.fmt.allocPrint(allocator, "{s}-wal-shm", .{path});
    defer allocator.free(shm_path);
    std.fs.cwd().deleteFile(shm_path) catch {};
    defer std.fs.cwd().deleteFile(shm_path) catch {};

    const path_z = try allocator.dupeZ(u8, path);
    defer allocator.free(path_z);

    var bucket: *Bucket = undefined;
    try testing.expectEqual(Result.OK, albedo_open(path_z.ptr, &bucket));
    defer _ = albedo_close(bucket);

    var sub_query = try bson.BSONDocument.fromJSON(allocator, "{}");
    defer sub_query.deinit(allocator);

    var sub_handle: *SubscriptionHandle = undefined;
    try testing.expectEqual(Result.OK, albedo_subscribe(bucket, @constCast(sub_query.buffer.ptr), &sub_handle));

    // On a fresh bucket with no inserts, seqno should be 0.
    try testing.expectEqual(@as(u64, 0), albedo_subscribe_seqno(sub_handle));

    // Poll on an idle bucket must return EOS without any allocation.
    var batch_ptr: [*]u8 = undefined;
    try testing.expectEqual(Result.EOS, albedo_subscribe_poll(sub_handle, &batch_ptr, 64));

    try testing.expectEqual(Result.OK, albedo_subscribe_close(sub_handle));
}

test "lib API close list handle without exhausting does not leak" {
    const allocator = testing.allocator;
    const path = try makeTempPath(allocator, "lib-api-list-early-close");
    defer allocator.free(path);
    platform.deleteFile(path) catch {};
    defer platform.deleteFile(path) catch {};

    const path_z = try allocator.dupeZ(u8, path);
    defer allocator.free(path_z);

    var bucket: *Bucket = undefined;
    try testing.expectEqual(Result.OK, albedo_open(path_z.ptr, &bucket));
    defer _ = albedo_close(bucket);

    for ([_][]const u8{ "X", "Y", "Z" }) |name| {
        const json = try std.fmt.allocPrint(allocator, "{{\"name\":\"{s}\"}}", .{name});
        defer allocator.free(json);
        var doc = try bson.BSONDocument.fromJSON(allocator, json);
        defer doc.deinit(allocator);
        try testing.expectEqual(Result.OK, albedo_insert(bucket, @constCast(doc.buffer.ptr)));
    }

    var list_query = try bson.BSONDocument.fromJSON(allocator, "{}");
    defer list_query.deinit(allocator);

    var handle: *ListHandle = undefined;
    try testing.expectEqual(Result.OK, albedo_list(bucket, @constCast(list_query.buffer.ptr), &handle));

    // Read only the first document, then close early — the arena must still
    // be fully freed by albedo_close_iterator.
    var raw_doc_ptr: [*]u8 = undefined;
    try testing.expectEqual(Result.OK, albedo_data(handle, &raw_doc_ptr));

    try testing.expectEqual(Result.OK, albedo_close_iterator(handle));
}

test "lib API transform close without apply does not leak" {
    const allocator = testing.allocator;
    const path = try makeTempPath(allocator, "lib-api-transform-early-close");
    defer allocator.free(path);
    platform.deleteFile(path) catch {};
    defer platform.deleteFile(path) catch {};

    const path_z = try allocator.dupeZ(u8, path);
    defer allocator.free(path_z);

    var bucket: *Bucket = undefined;
    try testing.expectEqual(Result.OK, albedo_open(path_z.ptr, &bucket));
    defer _ = albedo_close(bucket);

    var doc = try bson.BSONDocument.fromJSON(allocator, "{\"name\":\"Frank\"}");
    defer doc.deinit(allocator);
    try testing.expectEqual(Result.OK, albedo_insert(bucket, @constCast(doc.buffer.ptr)));

    var transform_query = try bson.BSONDocument.fromJSON(allocator,
        \\{"query":{"name":"Frank"}}
    );
    defer transform_query.deinit(allocator);

    var iterator: *Bucket.TransformIterator = undefined;
    try testing.expectEqual(Result.OK, albedo_transform(bucket, @constCast(transform_query.buffer.ptr), &iterator));

    // Fetch the current document pointer but skip calling albedo_transform_apply.
    // albedo_transform_close must release the iterator arena regardless.
    var current_ptr: [*c]u8 = undefined;
    try testing.expectEqual(Result.OK, albedo_transform_data(iterator, &current_ptr));

    try testing.expectEqual(Result.OK, albedo_transform_close(iterator));
}
