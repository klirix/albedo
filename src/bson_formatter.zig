const std = @import("std");
const bson = @import("./bson.zig");
const ObjectId = @import("./object_id.zig").ObjectId;

pub const Error = error{
    UnsupportedType,
    MissingField,
    TypeMismatch,
    IntegerOverflow,
};

pub fn ParseResult(comptime T: type) type {
    return struct {
        value: T,
        arena: std.heap.ArenaAllocator,

        pub fn deinit(self: *@This()) void {
            self.arena.deinit();
        }
    };
}

pub fn serialize(value: anytype, allocator: std.mem.Allocator) (Error || std.mem.Allocator.Error)!bson.BSONDocument {
    return try serializeStruct(@TypeOf(value), value, allocator);
}

pub fn parse(comptime T: type, doc: bson.BSONDocument, allocator: std.mem.Allocator) (Error || std.mem.Allocator.Error)!ParseResult(T) {
    var arena = std.heap.ArenaAllocator.init(allocator);
    errdefer arena.deinit();

    const parsed_value = try parseDocumentToType(T, doc, arena.allocator());
    return .{
        .value = parsed_value,
        .arena = arena,
    };
}

fn serializeStruct(comptime T: type, value: T, allocator: std.mem.Allocator) (Error || std.mem.Allocator.Error)!bson.BSONDocument {
    const type_info = @typeInfo(T);
    if (type_info != .@"struct") return Error.UnsupportedType;

    const fields = type_info.@"struct".fields;
    var pairs: [fields.len]bson.BSONKeyValuePair = undefined;
    var nested_docs = std.ArrayList(bson.BSONDocument){};
    defer {
        for (nested_docs.items) |doc| {
            doc.deinit(allocator);
        }
        nested_docs.deinit(allocator);
    }

    inline for (fields, 0..) |field, idx| {
        const field_value = @field(value, field.name);
        pairs[idx] = .{
            .key = field.name,
            .value = try toBsonValue(field.type, field_value, allocator, &nested_docs),
        };
    }

    return try bson.BSONDocument.fromPairs(allocator, pairs[0..]);
}

fn toBsonValue(
    comptime T: type,
    value: T,
    allocator: std.mem.Allocator,
    nested_docs: *std.ArrayList(bson.BSONDocument),
) (Error || std.mem.Allocator.Error)!bson.BSONValue {
    if (bson.BSONValue.fromNative(T, value) catch |err| switch (err) {
        error.IntegerOverflow => return Error.IntegerOverflow,
    }) |native| {
        return native;
    }

    return switch (@typeInfo(T)) {
        .optional => |opt| if (value) |inner| try toBsonValue(opt.child, inner, allocator, nested_docs) else bson.BSONValue{ .null = .{} },
        .pointer => |ptr| switch (ptr.size) {
            .slice => if (ptr.child != u8) blk: {
                const array_doc = try sliceToBsonArray(ptr.child, value, allocator, nested_docs);
                try nested_docs.append(allocator, array_doc);
                break :blk bson.BSONValue{ .array = array_doc };
            } else Error.UnsupportedType,
            .one => Error.UnsupportedType,
            else => Error.UnsupportedType,
        },
        .array => |arr| if (arr.child != u8 or arr.sentinel() == null) blk: {
            const array_doc = try sliceToBsonArray(arr.child, value[0..], allocator, nested_docs);
            try nested_docs.append(allocator, array_doc);
            break :blk bson.BSONValue{ .array = array_doc };
        } else Error.UnsupportedType,
        .@"struct" => blk: {
            const nested_doc = try serializeStruct(T, value, allocator);
            try nested_docs.append(allocator, nested_doc);
            break :blk bson.BSONValue{ .document = nested_doc };
        },
        else => Error.UnsupportedType,
    };
}

fn parseDocumentToType(comptime T: type, doc: bson.BSONDocument, arena_allocator: std.mem.Allocator) (Error || std.mem.Allocator.Error)!T {
    const type_info = @typeInfo(T);
    if (type_info != .@"struct") return Error.UnsupportedType;

    var result: T = undefined;
    inline for (type_info.@"struct".fields) |field| {
        const field_type = field.type;
        const maybe_value = doc.get(field.name);

        if (maybe_value) |bson_value| {
            @field(result, field.name) = try parseValueToType(field_type, bson_value, arena_allocator);
        } else {
            if (@typeInfo(field_type) == .optional) {
                @field(result, field.name) = null;
            } else {
                return Error.MissingField;
            }
        }
    }

    return result;
}

fn parseValueToType(comptime T: type, bson_value: bson.BSONValue, arena_allocator: std.mem.Allocator) (Error || std.mem.Allocator.Error)!T {
    return switch (@typeInfo(T)) {
        .bool => switch (bson_value) {
            .boolean => bson_value.boolean.value,
            else => Error.TypeMismatch,
        },
        .float => switch (bson_value) {
            .double => @as(T, @floatCast(bson_value.double.value)),
            .int32 => @as(T, @floatFromInt(bson_value.int32.value)),
            .int64 => @as(T, @floatFromInt(bson_value.int64.value)),
            else => Error.TypeMismatch,
        },
        .int => switch (bson_value) {
            .int32 => std.math.cast(T, bson_value.int32.value) orelse Error.IntegerOverflow,
            .int64 => std.math.cast(T, bson_value.int64.value) orelse Error.IntegerOverflow,
            .double => std.math.cast(T, @as(i64, @intFromFloat(bson_value.double.value))) orelse Error.IntegerOverflow,
            else => Error.TypeMismatch,
        },
        .optional => |opt| switch (bson_value) {
            .null => null,
            else => try parseValueToType(opt.child, bson_value, arena_allocator),
        },
        .pointer => |ptr| switch (ptr.size) {
            .slice => if (ptr.child == u8)
                switch (bson_value) {
                    .string => try arena_allocator.dupe(u8, bson_value.string.value),
                    else => Error.TypeMismatch,
                }
            else switch (bson_value) {
                .array => try parseBsonArrayToSlice(ptr.child, bson_value.array, arena_allocator),
                else => Error.TypeMismatch,
            },
            else => Error.UnsupportedType,
        },
        .array => |arr| if (arr.child == u8 and arr.sentinel() != null)
            switch (bson_value) {
                .string => try arena_allocator.dupeZ(u8, bson_value.string.value),
                else => Error.TypeMismatch,
            }
        else switch (bson_value) {
            .array => try parseBsonArrayToFixedArray(T, arr.child, bson_value.array, arena_allocator),
            else => Error.TypeMismatch,
        },
        .@"struct" => if (T == ObjectId)
            switch (bson_value) {
                .objectId => bson_value.objectId.value,
                else => Error.TypeMismatch,
            }
        else switch (bson_value) {
            .document => try parseDocumentToType(T, bson_value.document, arena_allocator),
            else => Error.TypeMismatch,
        },
        else => Error.UnsupportedType,
    };
}

fn sliceToBsonArray(
    comptime Elem: type,
    values: []const Elem,
    allocator: std.mem.Allocator,
    nested_docs: *std.ArrayList(bson.BSONDocument),
) (Error || std.mem.Allocator.Error)!bson.BSONDocument {
    const pairs = try allocator.alloc(bson.BSONKeyValuePair, values.len);
    defer allocator.free(pairs);

    const keys = try allocator.alloc([]u8, values.len);
    defer {
        for (keys) |key| {
            allocator.free(key);
        }
        allocator.free(keys);
    }

    for (values, 0..) |item, idx| {
        keys[idx] = try std.fmt.allocPrint(allocator, "{d}", .{idx});
        pairs[idx] = .{
            .key = keys[idx],
            .value = try toBsonValue(Elem, item, allocator, nested_docs),
        };
    }

    return try bson.BSONDocument.fromPairs(allocator, pairs);
}

fn parseBsonArrayToSlice(
    comptime Elem: type,
    array_doc: bson.BSONDocument,
    arena_allocator: std.mem.Allocator,
) (Error || std.mem.Allocator.Error)![]Elem {
    const len: usize = @intCast(array_doc.keyNumber());
    const out = try arena_allocator.alloc(Elem, len);

    for (0..len) |idx| {
        var key_buf: [20]u8 = undefined;
        const key = std.fmt.bufPrint(&key_buf, "{d}", .{idx}) catch unreachable;
        const value = array_doc.get(key) orelse return Error.MissingField;
        out[idx] = try parseValueToType(Elem, value, arena_allocator);
    }

    return out;
}

fn parseBsonArrayToFixedArray(
    comptime ArrT: type,
    comptime Elem: type,
    array_doc: bson.BSONDocument,
    arena_allocator: std.mem.Allocator,
) (Error || std.mem.Allocator.Error)!ArrT {
    var out: ArrT = undefined;

    for (0..out.len) |idx| {
        var key_buf: [20]u8 = undefined;
        const key = std.fmt.bufPrint(&key_buf, "{d}", .{idx}) catch unreachable;
        const value = array_doc.get(key) orelse return Error.MissingField;
        out[idx] = try parseValueToType(Elem, value, arena_allocator);
    }

    return out;
}

test "serialize/parse struct roundtrip" {
    const T = struct {
        name: []const u8,
        age: i32,
    };

    var doc = try serialize(T{ .name = "Alice", .age = 32 }, std.testing.allocator);
    defer doc.deinit(std.testing.allocator);

    var parsed = try parse(T, doc, std.testing.allocator);
    defer parsed.deinit();

    try std.testing.expectEqualStrings("Alice", parsed.value.name);
    try std.testing.expectEqual(@as(i32, 32), parsed.value.age);
}

test "parse optional missing field" {
    const T = struct {
        name: []const u8,
        age: ?i32,
    };

    var doc = try serialize(.{ .name = "Alice", .age = @as(?i32, null) }, std.testing.allocator);
    defer doc.deinit(std.testing.allocator);

    var parsed = try parse(T, doc, std.testing.allocator);
    defer parsed.deinit();

    try std.testing.expectEqualStrings("Alice", parsed.value.name);
    try std.testing.expectEqual(@as(?i32, null), parsed.value.age);
}

test "serialize/parse nested struct roundtrip" {
    const T = struct {
        name: []const u8,
        profile: struct {
            age: i32,
            city: []const u8,
        },
    };

    var doc = try serialize(T{
        .name = "Alice",
        .profile = .{
            .age = 32,
            .city = "Paris",
        },
    }, std.testing.allocator);
    defer doc.deinit(std.testing.allocator);

    var parsed = try parse(T, doc, std.testing.allocator);
    defer parsed.deinit();

    try std.testing.expectEqualStrings("Alice", parsed.value.name);
    try std.testing.expectEqual(@as(i32, 32), parsed.value.profile.age);
    try std.testing.expectEqualStrings("Paris", parsed.value.profile.city);
}

test "serialize/parse nested tuple roundtrip" {
    const T = struct {
        name: []const u8,
        profile: struct {
            age: i32,
            city: []const u8,
        },
    };

    var doc = try serialize(T{
        .name = "Alice",
        .profile = .{
            .age = 32,
            .city = "Paris",
        },
    }, std.testing.allocator);
    defer doc.deinit(std.testing.allocator);

    var parsed = try parse(T, doc, std.testing.allocator);
    defer parsed.deinit();

    try std.testing.expectEqualStrings("Alice", parsed.value.name);
    try std.testing.expectEqual(@as(i32, 32), parsed.value.profile.age);
    try std.testing.expectEqualStrings("Paris", parsed.value.profile.city);
}

test "serialize/parse nested fixed array in struct" {
    const T = struct {
        name: []const u8,
        profile: struct {
            age: i32,
            scores: [3]i32,
        },
    };

    var doc = try serialize(T{
        .name = "Alice",
        .profile = .{
            .age = 32,
            .scores = .{ 10, 20, 30 },
        },
    }, std.testing.allocator);
    defer doc.deinit(std.testing.allocator);

    var parsed = try parse(T, doc, std.testing.allocator);
    defer parsed.deinit();

    try std.testing.expectEqualStrings("Alice", parsed.value.name);
    try std.testing.expectEqual(@as(i32, 32), parsed.value.profile.age);
    try std.testing.expectEqual(@as(i32, 10), parsed.value.profile.scores[0]);
    try std.testing.expectEqual(@as(i32, 20), parsed.value.profile.scores[1]);
    try std.testing.expectEqual(@as(i32, 30), parsed.value.profile.scores[2]);
}

test "serialize/parse nested slice in struct" {
    const T = struct {
        name: []const u8,
        profile: struct {
            tags: []const []const u8,
        },
    };

    const tags = [_][]const u8{ "db", "zig", "bson" };
    var doc = try serialize(T{
        .name = "Alice",
        .profile = .{
            .tags = tags[0..],
        },
    }, std.testing.allocator);
    defer doc.deinit(std.testing.allocator);

    var parsed = try parse(T, doc, std.testing.allocator);
    defer parsed.deinit();

    try std.testing.expectEqualStrings("Alice", parsed.value.name);
    try std.testing.expectEqual(@as(usize, 3), parsed.value.profile.tags.len);
    try std.testing.expectEqualStrings("db", parsed.value.profile.tags[0]);
    try std.testing.expectEqualStrings("zig", parsed.value.profile.tags[1]);
    try std.testing.expectEqualStrings("bson", parsed.value.profile.tags[2]);
}
