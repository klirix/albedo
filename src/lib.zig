const std = @import("std");
const testing = std.testing;
const albedo = @import("./albedo.zig");
const bson = @import("./bson.zig");
const platform = @import("./platform.zig");
const Query = @import("./query.zig").Query;
const IndexOptions = @import("./bplusindex.zig").IndexOptions;

const ally = if (platform.isWasm) std.heap.wasm_allocator else std.heap.smp_allocator;

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
};

pub export fn albedo_open(path: [*:0]u8, out: **albedo.Bucket) Result {
    const pathProper = std.mem.span(path);
    // var gpa = std.heap.GeneralPurposeAllocator(.{}).init;
    // defer _ = gpa.deinit();
    const db = ally.create(albedo.Bucket) catch return Result.OutOfMemory;
    db.* = albedo.Bucket.init(ally, pathProper) catch return Result.Error;
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
    const docBufferProper = queryBuffer[0..queryLen];

    const query = Query.parseRaw(bucket.allocator, docBufferProper) catch |err| switch (err) {
        Query.QueryParsingErrors.OutOfMemory => {
            return Result.OutOfMemory;
        },
        else => {
            return Result.Error;
        },
    };

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

const ListHandle = struct {
    iterator: *ListIterator,
    arena: *std.heap.ArenaAllocator,
};

pub export fn albedo_list(bucket: *albedo.Bucket, queryBuffer: [*]u8, outIterator: **ListHandle) Result {
    const queryLen = std.mem.readInt(u32, queryBuffer[0..4], .little);
    const queryBufferProper = queryBuffer[0..queryLen];

    var arena = std.heap.ArenaAllocator.init(bucket.allocator);
    const queryArena = arena.allocator().create(std.heap.ArenaAllocator) catch return Result.OutOfMemory;
    queryArena.* = arena;
    // const queryArenaAllocator = queryArena.allocator();

    const query = Query.parseRaw(queryArena.allocator(), queryBufferProper) catch |err| switch (err) {
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
    const listHandle = bucket.allocator.create(ListHandle) catch return Result.OutOfMemory;
    listHandle.* = ListHandle{
        .iterator = iterator,
        .arena = queryArena,
    };
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

/// Advances the iterator to the next result and returns the current state.
/// If the iterator is at the end of the results, it returns `Result.EOS`.
/// Otherwise, it returns `Result.Data` to indicate more data is available.
pub export fn albedo_next(handle: *ListHandle) Result {
    if (handle.iterator.index == 0) {
        return Result.EOS;
    }
    return Result.HasData;
}

pub export fn albedo_close_iterator(iterator: *ListHandle) Result {
    iterator.arena.deinit();
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

// test "basic add functionality" {
//     try testing.expect(add(3, 7) == 10);
// }
