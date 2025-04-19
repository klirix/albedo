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
    Data,
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
        else => {
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

pub export fn albedo_insert(bucket: *albedo.Bucket, docBuffer: [*:0]u8) Result {
    const docBufferProper = std.mem.span(docBuffer);

    var doc = bson.BSONDocument.deserializeFromMemory(
        allocator,
        docBufferProper,
    ) catch |err| switch (err) {
        else => {
            return Result.Error;
        },
    };
    defer doc.deinit();

    _ = bucket.insert(&doc) catch |err| switch (err) {
        else => {
            return Result.Error;
        },
    };

    return Result.OK;
    // Insert the document into the bucket
    // This is a placeholder for actual insertion logic
}

pub export fn albedo_delete(bucket: *albedo.Bucket, docBuffer: [*:0]u8) Result {
    const docBufferProper = std.mem.span(docBuffer);

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
};

pub export fn albedo_list(bucket: *albedo.Bucket, queryBuffer: [*:0]u8, outIterator: **RequestIterator) Result {
    const queryBufferProper = std.mem.span(queryBuffer);
    var queryArena = std.heap.ArenaAllocator.init(allocator);
    const queryArenaAllocator = queryArena.allocator();
    var queryDoc = bson.BSONDocument.deserializeFromMemory(
        bucket.allocator,
        queryBufferProper,
    ) catch return Result.OutOfMemory;
    defer queryDoc.deinit();
    const query = Query.parseRaw(queryArenaAllocator, queryBufferProper) catch |err| switch (err) {
        else => {
            return Result.Error;
        },
    };

    const results = bucket.list(queryArenaAllocator, query) catch |err| switch (err) {
        else => {
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
    return doc.len;
}

pub export fn albedo_data(iterator: *RequestIterator, outDoc: [*]u8) Result {
    if (iterator.idx >= iterator.results.len) {
        return Result.Error;
    }
    const doc = iterator.results[iterator.idx];
    iterator.idx += 1;

    // Transform the outDoc pointer into a slice of the size of the current document
    const outDocSlice = outDoc[0..doc.len];

    // Serialize the BSON document into memory
    doc.serializeToMemory(outDocSlice);

    return Result.OK;
}

pub export fn albedo_next(iterator: *RequestIterator) Result {
    if (iterator.idx >= iterator.results.len) {
        return Result.EOS;
    }
    iterator.idx += 1;
    return Result.Data;
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
