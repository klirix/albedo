const napigen = @import("napigen");
const std = @import("std");
const testing = std.testing;
const albedo = @import("./albedo.zig");
const Query = @import("./query.zig").Query;
const bson = @import("./bson.zig");

const allocator = std.heap.page_allocator;

const Bucket = albedo.Bucket;

comptime {
    napigen.defineModule(initModule);
}

fn initModule(js: *napigen.JsContext, exports: napigen.napi_value) anyerror!napigen.napi_value {
    // try js.setNamedProperty(exports, "add", try js.createFunction(bsonToNapi));
    try js.setNamedProperty(exports, "open", try js.createFunction(open));
    try js.setNamedProperty(exports, "close", try js.createFunction(close));
    try js.setNamedProperty(exports, "insert", try js.createFunction(insert));
    try js.setNamedProperty(exports, "list", try js.createFunction(list));
    try js.setNamedProperty(exports, "all", try js.createFunction(all));
    try js.setNamedProperty(exports, "iter_next", try js.createFunction(iter_next));
    try js.setNamedProperty(exports, "iter_close", try js.createFunction(iter_close));
    try js.setNamedProperty(exports, "delete", try js.createFunction(delete));
    try js.setNamedProperty(exports, "vacuum", try js.createFunction(vacuum));

    return exports;
}

fn open(name: []const u8) !*Bucket {
    const db = try allocator.create(Bucket);
    db.* = try Bucket.init(allocator, name);
    return db;
}

fn close(db: *Bucket) !void {
    db.deinit();
    allocator.destroy(db);
}

fn vacuum(db: *Bucket) !void {
    try db.vacuum();
}

fn insert(js: *napigen.JsContext, bucket: *Bucket, object: napigen.napi_value) !void {
    var insertee = try jsObjectToBSON(js, object);
    defer insertee.deinit(allocator);
    _ = try bucket.insert(insertee);
}

const RequestIterator = struct {
    idx: usize = 0,
    bucket: *albedo.Bucket,
    results: []bson.BSONDocument,
    arena: std.heap.ArenaAllocator,
    new: bool = true,
};

fn all(js: *napigen.JsContext, bucket: *Bucket, queryJS: napigen.napi_value) !napigen.napi_value {
    var queryArena = std.heap.ArenaAllocator.init(allocator);
    const queryArenaAllocator = queryArena.allocator();

    const queryDoc = try jsObjectToBSON(js, queryJS);
    const query = try Query.parse(queryArenaAllocator, queryDoc);

    const initial = try std.time.Instant.now();
    const result = try bucket.list(queryArenaAllocator, query);
    if (result.len > std.math.maxInt(u32)) {
        return error.TooManyResults;
    }
    const elapsed = try std.time.Instant.now();
    std.debug.print("Query ran for: {d}ms\n", .{@divFloor(elapsed.since(initial), 1_000_000)});
    const resultArray = try js.createArrayWithLength(@truncate(result.len));
    for (result, 0..) |value, i| {
        const doc = try bsonDocToJS(js, queryArenaAllocator, value);
        try js.setElement(resultArray, @truncate(i), doc);
        if (@mod(i, 10000) == 0) {
            const partialElapsed = try std.time.Instant.now();
            std.debug.print("Processed {d} documents in {d}ms \n", .{ i, @divFloor(partialElapsed.since(elapsed), 1_000_000) });
        }
    }
    return resultArray;
}

fn list(js: *napigen.JsContext, bucket: *Bucket, queryJS: napigen.napi_value) !*RequestIterator {
    var queryArena = std.heap.ArenaAllocator.init(allocator);
    const queryArenaAllocator = queryArena.allocator();

    const queryDoc = try jsObjectToBSON(js, queryJS);
    const query = try Query.parse(queryArenaAllocator, queryDoc);

    const iter = try queryArenaAllocator.create(RequestIterator);
    iter.* = RequestIterator{
        .idx = 0,
        .bucket = bucket,
        .results = try bucket.list(queryArenaAllocator, query),
        .arena = queryArena,
        .new = true,
    };

    return iter;
}

fn iter_close(iter: *RequestIterator) !void {
    iter.arena.deinit();
}

fn delete(js: *napigen.JsContext, bucket: *Bucket, queryJS: napigen.napi_value) !void {
    var queryArena = std.heap.ArenaAllocator.init(allocator);
    const queryArenaAllocator = queryArena.allocator();

    const queryDoc = try jsObjectToBSON(js, queryJS);
    const query = try Query.parse(queryArenaAllocator, queryDoc);

    try bucket.delete(query);
}

fn iter_next(js: *napigen.JsContext, iter: *RequestIterator) !napigen.napi_value {
    if (iter.idx >= iter.results.len) {
        return try js.null();
    }
    const doc = iter.results[iter.idx];

    defer iter.idx += 1;
    return try bsonDocToJS(js, iter.arena.allocator(), doc);
}

fn jsObjectToBSON(js: *napigen.JsContext, object: napigen.napi_value) !bson.BSONDocument {
    var docBuff = std.ArrayList(u8).init(allocator);
    var writer = docBuff.writer();
    try writer.writeInt(u32, 0, .little); // Placeholder for length
    var keys: napigen.napi_value = undefined;
    try napigen.check(napigen.napi_get_all_property_names(
        js.env,
        object,
        napigen.napi_key_own_only,
        napigen.napi_key_skip_symbols,
        16,
        &keys,
    ));
    const keyCount = try js.getArrayLength(keys);
    for (0..keyCount) |i| {
        const key = try js.getElement(keys, @truncate(i));
        const keyString: [:0]const u8 = @ptrCast(try js.readString(key));
        const value = try js.getNamedProperty(object, keyString.ptr);
        const bsonVal: bson.BSONValue = switch (try js.typeOf(value)) {
            napigen.napi_undefined => continue,
            napigen.napi_null => bson.BSONValue{ .null = .{} },
            napigen.napi_boolean => bson.BSONValue{ .boolean = .{ .value = try js.readBoolean(value) } },
            napigen.napi_number => bson.BSONValue{ .double = .{ .value = try js.readNumber(f64, value) } },
            napigen.napi_string => bson.BSONValue{ .string = .{ .value = try js.readString(value) } },
            napigen.napi_symbol => continue,
            napigen.napi_object => obj: {
                const doc = try jsObjectToBSON(js, value);
                var isArray: bool = undefined;
                try napigen.check(napigen.napi_is_array(js.env, value, &isArray));
                if (isArray) {
                    break :obj bson.BSONValue{ .array = doc };
                } else {
                    break :obj bson.BSONValue{ .document = doc };
                }
            },
            napigen.napi_function => continue,
            napigen.napi_external => continue,
            napigen.napi_bigint => bson.BSONValue{ .int64 = .{ .value = try js.readNumber(i64, value) } },
            else => unreachable,
        };
        try writer.writeByte(@intFromEnum(bsonVal.valueType()));
        try writer.writeAll(keyString);
        try writer.writeByte(0);
        try bsonVal.write(writer);
    }
    try writer.writeByte(0); // Null terminator
    const docSize: u32 = @truncate(docBuff.items.len);
    std.mem.writeInt(u32, docBuff.items[0..4], docSize, .little);
    return .{ .buffer = try docBuff.toOwnedSlice() };
}

// -- internal

fn bsonDocToJS(js: *napigen.JsContext, ally: std.mem.Allocator, object: bson.BSONDocument) anyerror!napigen.napi_value {

    // Convert BSON document to NAPI value
    // This is a placeholder for actual conversion logic
    const obj = try js.createObject();
    var iter = object.iter();
    while (iter.next()) |pair| {
        const keyTerminated = try std.fmt.allocPrintZ(ally, "{s}", .{pair.key});
        try js.setNamedProperty(obj, keyTerminated.ptr, switch (pair.value) {
            .document => |doc| try bsonDocToJS(js, ally, doc),
            .array => |arr| try bsonArrayToJS(js, ally, arr),
            else => try scalarToJS(js, pair.value),
        });
    }
    return obj;
}

fn bsonArrayToJS(js: *napigen.JsContext, ally: std.mem.Allocator, object: bson.BSONDocument) anyerror!napigen.napi_value {

    // Convert BSON document to NAPI value
    // This is a placeholder for actual conversion logic
    const obj = try js.createArrayWithLength(object.keyNumber());
    var iter = object.iter();
    var index: u32 = 0;
    while (iter.next()) |pair| : (index += 1) {
        try js.setElement(obj, index, switch (pair.value) {
            .document => |doc| try bsonDocToJS(js, ally, doc),
            .array => |arr| try bsonArrayToJS(js, ally, arr),
            else => try scalarToJS(js, pair.value),
        });
    }
    return obj;
}

fn scalarToJS(js: *napigen.JsContext, scalar: bson.BSONValue) !napigen.napi_value {
    return switch (scalar) {
        .string => |s| try js.createString(s.value),
        .int32 => |i| try js.createNumber(i.value),
        .int64 => |i| try js.createNumber(i.value),
        .binary => |b| b: {
            var data: napigen.napi_value = undefined;
            var pointerToData: [*c]u8 = undefined;
            try napigen.check(napigen.napi_create_arraybuffer(js.env, b.value.len, &pointerToData, &data));
            @memcpy(pointerToData[0..b.value.len], b.value.ptr);
            break :b data;
        },
        .double => |d| try js.createNumber(d.value),
        .boolean => |b| try js.createBoolean(b.value),
        .null => try js.null(),
        .datetime => |dt| b: {
            var data: napigen.napi_value = undefined;
            try napigen.check(napigen.napi_create_date(js.env, @as(f64, @floatFromInt(dt.value)), &data));
            break :b data;
        },
        .objectId => |oid| try js.createString(&oid.value.toString()),
        else => unreachable,
    };
}
