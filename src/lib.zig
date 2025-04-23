const std = @import("std");
const testing = std.testing;
const albedo = @import("./albedo.zig");
const bson = @import("./bson.zig");
const Query = @import("./query.zig").Query;

const allocator = std.heap.page_allocator;

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
    InvalidFormat,
};

pub export fn albedo_open(path: [*:0]u8, out: **albedo.Bucket) Result {
    const pathProper = std.mem.span(path);
    const db = allocator.create(albedo.Bucket) catch return Result.OutOfMemory;
    db.* = albedo.Bucket.init(allocator, pathProper) catch |err| switch (err) {
        else => |dbOpenErr| {
            std.debug.print("Failed to open db, {any}", .{dbOpenErr});
            return Result.Error;
        },
    };
    out.* = db;
    return Result.OK;
}

pub export fn albedo_close(bucket: *albedo.Bucket) Result {
    bucket.deinit();
    allocator.destroy(bucket);
    return Result.OK;
}

pub export fn albedo_insert(bucket: *albedo.Bucket, docBuffer: [*]u8) Result {
    const docSize = std.mem.readInt(u32, docBuffer[0..4], .little);
    const docBufferProper = docBuffer[0..docSize];

    var doc = bson.BSONDocument.init(docBufferProper);

    _ = bucket.insert(&doc) catch |err| switch (err) {
        else => {
            return Result.Error;
        },
    };

    return Result.OK;
    // Insert the document into the bucket
    // This is a placeholder for actual insertion logic
}

pub export fn albedo_delete(bucket: *albedo.Bucket, queryBuffer: [*]u8, queryLen: u16) Result {
    const docBufferProper = queryBuffer[0..queryLen];

    const query = Query.parseRaw(allocator, docBufferProper) catch |err| switch (err) {
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

const RequestIterator = struct {
    idx: usize = 0,
    bucket: *albedo.Bucket,
    results: []bson.BSONDocument,
    arena: std.heap.ArenaAllocator,
    new: bool = true,
};

pub export fn albedo_list(bucket: *albedo.Bucket, queryBuffer: [*]u8, outIterator: **RequestIterator) Result {
    const queryLen = std.mem.readInt(u32, queryBuffer[0..4], .little);
    const queryBufferProper = queryBuffer[0..queryLen];

    var queryArena = std.heap.ArenaAllocator.init(allocator);
    const queryArenaAllocator = queryArena.allocator();

    const query = Query.parseRaw(queryArenaAllocator, queryBufferProper) catch |err| switch (err) {
        else => |qErr| {
            std.debug.print("Failed to parse query, {any}", .{qErr});
            return Result.Error;
        },
    };

    const results = bucket.list(queryArenaAllocator, query) catch |err| switch (err) {
        else => |rErr| {
            std.debug.print("Failed to list documents, {any}", .{rErr});
            return Result.Error;
        },
    };

    const iterator = queryArenaAllocator.create(RequestIterator) catch return Result.OutOfMemory;
    iterator.* = RequestIterator{
        .idx = 0,
        .bucket = bucket,
        .results = results,
        .arena = queryArena,
    };
    outIterator.* = iterator;

    return Result.OK;
}

pub export fn albedo_data_size(iterator: *RequestIterator) u32 {
    if (iterator.idx >= iterator.results.len) {
        return 0;
    }
    const doc = iterator.results[iterator.idx];
    return @truncate(doc.buffer.len);
}

pub export fn albedo_data(iterator: *RequestIterator, outDoc: [*]u8) Result {
    if (iterator.idx >= iterator.results.len) {
        return Result.Error;
    }
    const doc = iterator.results[iterator.idx];

    // Transform the outDoc pointer into a slice of the size of the current document
    // const outDocSlice = outDoc[0..doc.len];

    // Serialize the BSON document into memory
    @memcpy(outDoc, doc.buffer);

    return Result.OK;
}

/// Advances the iterator to the next result and returns the current state.
/// If the iterator is at the end of the results, it returns `Result.EOS`.
/// Otherwise, it returns `Result.Data` to indicate more data is available.
pub export fn albedo_next(iterator: *RequestIterator) Result {
    if (iterator.results.len == 0) {
        return Result.EOS;
    }

    if (iterator.new) {
        iterator.new = false;
    } else {
        if (iterator.idx + 1 >= iterator.results.len) {
            return Result.EOS;
        }
        iterator.idx += 1;
    }

    return Result.HasData;
}

pub export fn albedo_close_iterator(iterator: *RequestIterator) Result {
    iterator.arena.deinit();
    return Result.OK;
}

pub export fn albedo_version() u32 {
    return 1;
}

// test "basic add functionality" {
//     try testing.expect(add(3, 7) == 10);
// }
