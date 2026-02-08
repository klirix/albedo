const std = @import("std");
const testing = std.testing;
const albedo = @import("./albedo.zig");
const bson = @import("./bson.zig");
const platform = @import("./platform.zig");
const Query = @import("./query.zig").Query;
const IndexOptions = @import("./bplusindex.zig").IndexOptions;
const builtin = @import("builtin");

const ally = if (builtin.is_test)
    std.testing.allocator
else if (platform.isWasm)
    std.heap.wasm_allocator
else
    std.heap.smp_allocator;

const Bucket = albedo.Bucket;

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
};

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

pub export fn albedo_close(bucket: *albedo.Bucket) Result {
    bucket.deinit();
    bucket.allocator.destroy(bucket);
    return Result.OK;
}

pub export fn albedo_insert(bucket: *albedo.Bucket, docBuffer: [*]u8) Result {
    const docSize = std.mem.readInt(u32, docBuffer[0..4], .little);
    const docBufferProper = docBuffer[0..docSize];

    const doc = bson.BSONDocument.init(docBufferProper);

    _ = bucket.insert(doc) catch |err| switch (err) {
        error.DuplicateKey => {
            return Result.DuplicateKey;
        },
        else => {
            return Result.Error;
        },
    };

    return Result.OK;
    // Insert the document into the bucket
    // This is a placeholder for actual insertion logic
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
        else => {
            // std.debug.print("Failed to drop index for path {s}, {any}\n", .{ path_proper, err });
            return Result.Error;
        },
    };

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

    bucket.delete(query) catch |err| switch (err) {
        else => {
            return Result.Error;
        },
    };

    return Result.OK;
    // Insert the document into the bucket
    // This is a placeholder for actual insertion logic
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

    const query = Query.parseRaw(local_ally, queryBufferProper) catch |err| switch (err) {
        else => {
            // std.debug.print("Failed to parse query, {any}", .{qErr});
            return Result.Error;
        },
    };

    const iterator = bucket.listIterate(queryArena, query) catch |err| switch (err) {
        else => {
            // std.debug.print("Failed to list documents, {any}", .{rErr});
            return Result.Error;
        },
    };
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

pub export fn albedo_close_iterator(iterator: *ListHandle) Result {
    const arena_ptr = iterator.arena;
    iterator.iterator.deinit() catch {};
    arena_ptr.deinit();
    ally.destroy(arena_ptr);

    return Result.OK;
}

pub export fn albedo_vacuum(bucket: *Bucket) Result {
    bucket.vacuum() catch {
        // std.debug.print("Failed to vacuum bucket, {any}", .{err});
        return Result.Error;
    };
    return Result.OK;
}

pub export fn albedo_flush(bucket: *Bucket) Result {
    bucket.flush() catch {
        return Result.Error;
    };
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

    const iter = bucket.transformIterate(arena_ptr, query) catch |err| switch (err) {
        else => {
            return Result.Error;
        },
    };

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

    iterator.transform(doc) catch |err| switch (err) {
        else => {
            // std.debug.print("Failed to transform document, {any}", .{err});
            return Result.Error;
        },
    };

    return Result.OK;
}

pub export fn albedo_transform_close(iterator: *Bucket.TransformIterator) Result {
    iterator.close() catch {
        return Result.Error;
    };
    return Result.OK;
}

/// Set a callback to be notified of page changes for replication
pub export fn albedo_set_replication_callback(
    bucket: *Bucket,
    callback: albedo.PageChangeCallback,
    context: ?*anyopaque,
) Result {
    bucket.replication_callback = callback;
    bucket.replication_context = context;
    return Result.OK;
}

/// Apply a replicated page to this bucket (for replicas)
pub export fn albedo_apply_batch(
    bucket: *Bucket,
    data: [*]const u8,
    data_size: u32,
    page_count: u32,
) Result {
    bucket.applyReplicatedBatch(data[0..data_size], page_count) catch {
        return Result.Error;
    };

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

test "lib API apply batch rejects invalid payload size" {
    const allocator = testing.allocator;
    const path = try makeTempPath(allocator, "lib-api-apply-batch-invalid-size");
    defer allocator.free(path);
    platform.deleteFile(path) catch {};
    defer platform.deleteFile(path) catch {};

    const path_z = try allocator.dupeZ(u8, path);
    defer allocator.free(path_z);

    var bucket: *Bucket = undefined;
    try testing.expectEqual(Result.OK, albedo_open(path_z.ptr, &bucket));
    defer _ = albedo_close(bucket);

    const bad = [_]u8{ 1, 2, 3, 4 };
    try testing.expectEqual(Result.Error, albedo_apply_batch(bucket, bad[0..].ptr, bad.len, 1));
}
